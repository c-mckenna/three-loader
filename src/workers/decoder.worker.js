import { handleMessage } from './decoder-worker-internal';

/*eslint-disable */
onmessage = handleMessage;
