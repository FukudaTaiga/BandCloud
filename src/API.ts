/* tslint:disable */
/* eslint-disable */
//  This file was automatically generated and should not be edited.

export type CreateStudioTableInput = {
  address?: string | null,
  erea?: Array< string | null > | null,
  fee?: string | null,
  group?: string | null,
  id?: string | null,
  name?: string | null,
  number_rooms?: string | null,
  phone?: string | null,
  station?: string | null,
  time?: string | null,
  url?: string | null,
  _version?: number | null,
};

export type ModelStudioTableConditionInput = {
  address?: ModelStringInput | null,
  erea?: ModelStringInput | null,
  fee?: ModelStringInput | null,
  group?: ModelStringInput | null,
  name?: ModelStringInput | null,
  number_rooms?: ModelStringInput | null,
  phone?: ModelStringInput | null,
  station?: ModelStringInput | null,
  time?: ModelStringInput | null,
  url?: ModelStringInput | null,
  and?: Array< ModelStudioTableConditionInput | null > | null,
  or?: Array< ModelStudioTableConditionInput | null > | null,
  not?: ModelStudioTableConditionInput | null,
};

export type ModelStringInput = {
  ne?: string | null,
  eq?: string | null,
  le?: string | null,
  lt?: string | null,
  ge?: string | null,
  gt?: string | null,
  contains?: string | null,
  notContains?: string | null,
  between?: Array< string | null > | null,
  beginsWith?: string | null,
  attributeExists?: boolean | null,
  attributeType?: ModelAttributeTypes | null,
  size?: ModelSizeInput | null,
};

export enum ModelAttributeTypes {
  binary = "binary",
  binarySet = "binarySet",
  bool = "bool",
  list = "list",
  map = "map",
  number = "number",
  numberSet = "numberSet",
  string = "string",
  stringSet = "stringSet",
  _null = "_null",
}


export type ModelSizeInput = {
  ne?: number | null,
  eq?: number | null,
  le?: number | null,
  lt?: number | null,
  ge?: number | null,
  gt?: number | null,
  between?: Array< number | null > | null,
};

export type StudioTable = {
  __typename: "StudioTable",
  address?: string | null,
  erea?: Array< string | null > | null,
  fee?: string | null,
  group?: string | null,
  id?: string,
  name?: string | null,
  number_rooms?: string | null,
  phone?: string | null,
  station?: string | null,
  time?: string | null,
  url?: string | null,
  _version?: number,
  _deleted?: boolean | null,
  _lastChangedAt?: number,
  createdAt?: string,
  updatedAt?: string,
};

export type UpdateStudioTableInput = {
  address?: string | null,
  erea?: Array< string | null > | null,
  fee?: string | null,
  group?: string | null,
  id: string,
  name?: string | null,
  number_rooms?: string | null,
  phone?: string | null,
  station?: string | null,
  time?: string | null,
  url?: string | null,
  _version?: number | null,
};

export type DeleteStudioTableInput = {
  id: string,
  _version?: number | null,
};

export type ModelStudioTableFilterInput = {
  address?: ModelStringInput | null,
  erea?: ModelStringInput | null,
  fee?: ModelStringInput | null,
  group?: ModelStringInput | null,
  id?: ModelIDInput | null,
  name?: ModelStringInput | null,
  number_rooms?: ModelStringInput | null,
  phone?: ModelStringInput | null,
  station?: ModelStringInput | null,
  time?: ModelStringInput | null,
  url?: ModelStringInput | null,
  and?: Array< ModelStudioTableFilterInput | null > | null,
  or?: Array< ModelStudioTableFilterInput | null > | null,
  not?: ModelStudioTableFilterInput | null,
};

export type ModelIDInput = {
  ne?: string | null,
  eq?: string | null,
  le?: string | null,
  lt?: string | null,
  ge?: string | null,
  gt?: string | null,
  contains?: string | null,
  notContains?: string | null,
  between?: Array< string | null > | null,
  beginsWith?: string | null,
  attributeExists?: boolean | null,
  attributeType?: ModelAttributeTypes | null,
  size?: ModelSizeInput | null,
};

export type ModelStudioTableConnection = {
  __typename: "ModelStudioTableConnection",
  items?:  Array<StudioTable | null > | null,
  nextToken?: string | null,
  startedAt?: number | null,
};

export type CreateStudioTableMutationVariables = {
  input?: CreateStudioTableInput,
  condition?: ModelStudioTableConditionInput | null,
};

export type CreateStudioTableMutation = {
  createStudioTable?:  {
    __typename: "StudioTable",
    address?: string | null,
    erea?: Array< string | null > | null,
    fee?: string | null,
    group?: string | null,
    id: string,
    name?: string | null,
    number_rooms?: string | null,
    phone?: string | null,
    station?: string | null,
    time?: string | null,
    url?: string | null,
    _version: number,
    _deleted?: boolean | null,
    _lastChangedAt: number,
    createdAt: string,
    updatedAt: string,
  } | null,
};

export type UpdateStudioTableMutationVariables = {
  input?: UpdateStudioTableInput,
  condition?: ModelStudioTableConditionInput | null,
};

export type UpdateStudioTableMutation = {
  updateStudioTable?:  {
    __typename: "StudioTable",
    address?: string | null,
    erea?: Array< string | null > | null,
    fee?: string | null,
    group?: string | null,
    id: string,
    name?: string | null,
    number_rooms?: string | null,
    phone?: string | null,
    station?: string | null,
    time?: string | null,
    url?: string | null,
    _version: number,
    _deleted?: boolean | null,
    _lastChangedAt: number,
    createdAt: string,
    updatedAt: string,
  } | null,
};

export type DeleteStudioTableMutationVariables = {
  input?: DeleteStudioTableInput,
  condition?: ModelStudioTableConditionInput | null,
};

export type DeleteStudioTableMutation = {
  deleteStudioTable?:  {
    __typename: "StudioTable",
    address?: string | null,
    erea?: Array< string | null > | null,
    fee?: string | null,
    group?: string | null,
    id: string,
    name?: string | null,
    number_rooms?: string | null,
    phone?: string | null,
    station?: string | null,
    time?: string | null,
    url?: string | null,
    _version: number,
    _deleted?: boolean | null,
    _lastChangedAt: number,
    createdAt: string,
    updatedAt: string,
  } | null,
};

export type SyncStudioTablesQueryVariables = {
  filter?: ModelStudioTableFilterInput | null,
  limit?: number | null,
  nextToken?: string | null,
  lastSync?: number | null,
};

export type SyncStudioTablesQuery = {
  syncStudioTables?:  {
    __typename: "ModelStudioTableConnection",
    items?:  Array< {
      __typename: "StudioTable",
      address?: string | null,
      erea?: Array< string | null > | null,
      fee?: string | null,
      group?: string | null,
      id: string,
      name?: string | null,
      number_rooms?: string | null,
      phone?: string | null,
      station?: string | null,
      time?: string | null,
      url?: string | null,
      _version: number,
      _deleted?: boolean | null,
      _lastChangedAt: number,
      createdAt: string,
      updatedAt: string,
    } | null > | null,
    nextToken?: string | null,
    startedAt?: number | null,
  } | null,
};

export type GetStudioTableQueryVariables = {
  id?: string,
};

export type GetStudioTableQuery = {
  getStudioTable?:  {
    __typename: "StudioTable",
    address?: string | null,
    erea?: Array< string | null > | null,
    fee?: string | null,
    group?: string | null,
    id: string,
    name?: string | null,
    number_rooms?: string | null,
    phone?: string | null,
    station?: string | null,
    time?: string | null,
    url?: string | null,
    _version: number,
    _deleted?: boolean | null,
    _lastChangedAt: number,
    createdAt: string,
    updatedAt: string,
  } | null,
};

export type ListStudioTablesQueryVariables = {
  filter?: ModelStudioTableFilterInput | null,
  limit?: number | null,
  nextToken?: string | null,
};

export type ListStudioTablesQuery = {
  listStudioTables?:  {
    __typename: "ModelStudioTableConnection",
    items?:  Array< {
      __typename: "StudioTable",
      address?: string | null,
      erea?: Array< string | null > | null,
      fee?: string | null,
      group?: string | null,
      id: string,
      name?: string | null,
      number_rooms?: string | null,
      phone?: string | null,
      station?: string | null,
      time?: string | null,
      url?: string | null,
      _version: number,
      _deleted?: boolean | null,
      _lastChangedAt: number,
      createdAt: string,
      updatedAt: string,
    } | null > | null,
    nextToken?: string | null,
    startedAt?: number | null,
  } | null,
};

export type OnCreateStudioTableSubscription = {
  onCreateStudioTable?:  {
    __typename: "StudioTable",
    address?: string | null,
    erea?: Array< string | null > | null,
    fee?: string | null,
    group?: string | null,
    id: string,
    name?: string | null,
    number_rooms?: string | null,
    phone?: string | null,
    station?: string | null,
    time?: string | null,
    url?: string | null,
    _version: number,
    _deleted?: boolean | null,
    _lastChangedAt: number,
    createdAt: string,
    updatedAt: string,
  } | null,
};

export type OnUpdateStudioTableSubscription = {
  onUpdateStudioTable?:  {
    __typename: "StudioTable",
    address?: string | null,
    erea?: Array< string | null > | null,
    fee?: string | null,
    group?: string | null,
    id: string,
    name?: string | null,
    number_rooms?: string | null,
    phone?: string | null,
    station?: string | null,
    time?: string | null,
    url?: string | null,
    _version: number,
    _deleted?: boolean | null,
    _lastChangedAt: number,
    createdAt: string,
    updatedAt: string,
  } | null,
};

export type OnDeleteStudioTableSubscription = {
  onDeleteStudioTable?:  {
    __typename: "StudioTable",
    address?: string | null,
    erea?: Array< string | null > | null,
    fee?: string | null,
    group?: string | null,
    id: string,
    name?: string | null,
    number_rooms?: string | null,
    phone?: string | null,
    station?: string | null,
    time?: string | null,
    url?: string | null,
    _version: number,
    _deleted?: boolean | null,
    _lastChangedAt: number,
    createdAt: string,
    updatedAt: string,
  } | null,
};
