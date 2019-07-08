import pyspark.sql.types as T


def walkthrough_schema(schema, prefix=None):
    '''Walks through the schema of a Spark dataframe recursively,
    appending the nested field names of any structs, before adding
    them to an array.

    Args:
        schema (pyspark.sql.types.StructType) The schema of the dataframe or field.
        prefix (str): The concatenated nested fields.

    Returns:
        fields (list): All the nested fields concatenated into strings
        (e.g. 'root.field_name_1.field_name_2').

    '''
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
