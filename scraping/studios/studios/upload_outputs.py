import json
import boto3


def upload_to_dynamodb(table, file_name):
    id_counter = 0
    with open(file_name, "r") as fin:
        with table.batch_writer() as batch:
            for line in fin:
                item = json.loads(line)
                item["id"] = str(id_counter)
                batch.put_item(
                    Item=item
                )
                id_counter += 1


if __name__ == "__main__":
    client = boto3.client("dynamodb")
    print(client.list_tables()["TableNames"])

    dynamodb = boto3.resource('dynamodb')
    table    = dynamodb.Table('StudioTable-5rxepx5t4bexlajszoiyaztcam-dev')
    upload_to_dynamodb(table=table, file_name="outputs/sagasuta_items.jsonl")