import fastavro
import json
from io import BytesIO

schema = fastavro.parse_schema({
    "type": "record",
    "name": "testing",
    "fields": [{"name": "email", "type": "string"}]
})

data_dict = {}
data_dict['email']='asindic@gmail.com'

with BytesIO() as buf:
    fastavro.writer(buf, schema,[data_dict])
    buf.seek(0)

    result = fastavro.reader(buf)

    print([email for email in result])
#


#bio.seek(0)

#result = list(fastavro.reader(bio))
#for r in result:
#    print(r)