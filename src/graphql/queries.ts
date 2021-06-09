/* tslint:disable */
/* eslint-disable */
// this is an auto generated file. This will be overwritten

export const syncStudioTables = /* GraphQL */ `
  query SyncStudioTables(
    $filter: ModelStudioTableFilterInput
    $limit: Int
    $nextToken: String
    $lastSync: AWSTimestamp
  ) {
    syncStudioTables(
      filter: $filter
      limit: $limit
      nextToken: $nextToken
      lastSync: $lastSync
    ) {
      items {
        address
        erea
        fee
        group
        id
        name
        number_rooms
        phone
        station
        time
        url
        _version
        _deleted
        _lastChangedAt
        createdAt
        updatedAt
      }
      nextToken
      startedAt
    }
  }
`;
export const getStudioTable = /* GraphQL */ `
  query GetStudioTable($id: ID!) {
    getStudioTable(id: $id) {
      address
      erea
      fee
      group
      id
      name
      number_rooms
      phone
      station
      time
      url
      _version
      _deleted
      _lastChangedAt
      createdAt
      updatedAt
    }
  }
`;
export const listStudioTables = /* GraphQL */ `
  query ListStudioTables(
    $filter: ModelStudioTableFilterInput
    $limit: Int
    $nextToken: String
  ) {
    listStudioTables(filter: $filter, limit: $limit, nextToken: $nextToken) {
      items {
        address
        erea
        fee
        group
        id
        name
        number_rooms
        phone
        station
        time
        url
        _version
        _deleted
        _lastChangedAt
        createdAt
        updatedAt
      }
      nextToken
      startedAt
    }
  }
`;
