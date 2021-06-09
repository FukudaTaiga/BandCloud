import json
import boto3


def upload_to_dynamodb(file_name):
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
    dynamodb = boto3.resource('dynamodb')
    table    = dynamodb.Table('StudioTable-nkbsw5ci6ndrpgz7gvawqp4dyq-dev')

    upload_to_dynamodb("outputs/sagasuta_items.jsonl")