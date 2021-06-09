import { ModelInit, MutableModel, PersistentModelConstructor } from "@aws-amplify/datastore";





export declare class StudioTable {
  readonly id: string;
  readonly address?: string;
  readonly erea?: (string | null)[];
  readonly fee?: string;
  readonly group?: string;
  readonly name?: string;
  readonly phone?: string;
  readonly station?: string;
  readonly time?: string;
  readonly url?: string;
  constructor(init: ModelInit<StudioTable>);
  static copyOf(source: StudioTable, mutator: (draft: MutableModel<StudioTable>) => MutableModel<StudioTable> | void): StudioTable;
}