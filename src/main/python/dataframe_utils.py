import pyspark.sql.types as T


def walkthrough_schema(schema, prefix=None):
   
    fields = []
    for f in schema.fields:
        if not prefix:
            field_name = f.name
        else:
            field_name = "{}.{}".format(prefix, f.name)

        if isinstance(f.dataType, T.StructType):
            fields += walkthrough_schema(f.dataType, field_name)
        else:
            fields.append(field_name)
    return fields
