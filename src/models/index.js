// @ts-check
import { initSchema } from '@aws-amplify/datastore';
import { schema } from './schema';



const { StudioTable } = initSchema(schema);

export {
  StudioTable
};