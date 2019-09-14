import axios from 'axios';
import * as retry from 'retry';
import * as cluster from 'cluster';
import * as os from 'os';

interface PageInfo {
  title: string
}

interface WikiResponse {
  continue?: {
    apcontinue: string,
    continue: string
  },
  limits?: {
    allpages?: number
  },
  query: {
    allpages?: PageInfo[],
    namespaces: {
      [id: string]: {}
    }
  }
}

interface ProcessingInfo {
  namespace: number,
  amount: number
}

const env = process.env.NODE_ENV ? process.env.NODE_ENV : 'dev';

const baseUrl = env === 'wikipedia'
  ? 'https://en.wikipedia.org/w/api.php?origin=*'
  : 'https://www.mediawiki.org/w/api.php?origin=*';

const baseReqParams: { [key: string]: string } = {
  action: 'query',
  format: 'json',
  list: 'allpages',
  aplimit: 'max'
};

const processingInfos: ProcessingInfo[] = [];
let namespaces: number[];

const params = (namespace: number, from: string = null): string => {
  return Object.keys(baseReqParams)
    .reduce((res: string, key: string): string => `${res}&${key}=${baseReqParams[key]}`, '')
    .concat(`&apnamespace=${namespace}`)
    .concat(from ? `&apfrom=${encodeURIComponent(from)}` : '');
};

const reqWithRetries = async (url: string): Promise<unknown> => {
  const op = retry.operation({
    retries: 5,
    factor: 3,
    minTimeout: 200,
    maxTimeout: 2 * 1000,
    randomize: true
  });

  return new Promise((res, rej): void => {
    op.attempt(async () => {
      try {
        const { data }: { data: WikiResponse } = await axios.get(url);
        res(data);
      } catch (e) {
        if (!op.retry(e)) {
          rej(op.mainError());
        }
      }
    });
  });
};

const getPagesInfo = async (namespace: number, from: string): Promise<WikiResponse> => {
  return reqWithRetries(baseUrl + params(namespace, from)) as Promise<WikiResponse>;
};

const getNamespaces = async (): Promise<number[]> => {
  const url = `${baseUrl}&action=query&meta=siteinfo&siprop=namespaces&format=json`;
  const response = await reqWithRetries(url) as WikiResponse;

  return Object.keys(response.query.namespaces)
    .map((id: string): number => +id)
    .filter((id: number): boolean => id >= 0);
};

const processNamespace = async (namespace: number): Promise<ProcessingInfo> => {
  let from: string = null;
  let amount = 0;
  const log = () => console.log(`[${new Date().toISOString()}] Namespace ${namespace} processed ${amount} titles.`);

  do {
    const data: WikiResponse = await getPagesInfo(namespace, from);
    from = data.continue ? data.continue.apcontinue : null;

    //const pages = data.query.allpages.map(({ title }: PageInfo): string => title);
    // send(async) pages to kafka for example

    amount += data.limits.allpages;

    if (amount % 10000 === 0) {
      log();
    }
  } while (from);
  log();

  return {
    namespace,
    amount
  };
};

const subscribeToEvents = (child: cluster.Worker): void => {
  child.on('message', (info: ProcessingInfo): void => {
    processingInfos.push(info);
  });

  child.on('exit', (amount: number): void => {
    if (namespaces.length) {
      const child: cluster.Worker = cluster.fork({
        namespace: namespaces.pop()
      });
      subscribeToEvents(child);
    }

    if (Object.keys(cluster.workers).length === 0) {
      console.log(`Processing info: `);
      console.log(processingInfos);
      console.log(processingInfos.reduce((res: number, { amount } : ProcessingInfo) =>  res + amount, 0));
      process.exit();
    }
  });
};

const master = async (): Promise<void> => {
  namespaces = await getNamespaces();

  if (env === 'dev') {
    namespaces = namespaces.slice(0, 2);
  }

  const cpus: number = os.cpus().length;
  const namespacesAmount = namespaces.length;

  console.log(`Main process started. There are ${cpus} cores, ${cpus < namespacesAmount ? cpus : namespacesAmount} child processes will be started to process ${namespaces.length} namespaces.`);

  for(let i = 0; i < cpus && i < namespacesAmount; i++) {
    const child: cluster.Worker = cluster.fork({
      namespace: namespaces.pop()
    });
    subscribeToEvents(child);
  }
};

const child = async (): Promise<void> => {
  console.log(`Child process started, pid ${process.pid}, namespace: ${process.env.namespace}`);
  if (process.env.namespace) {
    const result: ProcessingInfo = await processNamespace(+process.env.namespace);

    process.send(result);
    process.exit();
  }
};

if (cluster.isMaster) {
  master();
} else {
  child();
}
