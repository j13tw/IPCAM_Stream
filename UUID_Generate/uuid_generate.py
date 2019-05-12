import uuid

generate_uuid = uuid.uuid4()
user_uuid = str(generate_uuid).upper().replace('-', '')
print(user_uuid)