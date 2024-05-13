import io
import json
from fdk import response
from helpers import initialize_process, load_sheets, first_transformation, second_transformation, third_transformation, fourth_transformation, fifth_transformation, sixth_transformation, extra_transformation, seventh_transformation, eight_transformation

"""
def handler(ctx, data: io.BytesIO=None):
    print("Entering Python Hello World handler", flush=True)
    name = "World"
    try:
        body = json.loads(data.getvalue())
        name = body.get("name")
    except (Exception, ValueError) as ex:
        print(str(ex), flush=True)

    print("Vale of name = ", name, flush=True)
    print("Exiting Python Hello World handler", flush=True)
    return response.Response(
        ctx, response_data=json.dumps(
            {"message": "Hello {0}".format(name)}),
        headers={"Content-Type": "application/json"}
    )
"""
def handelr(ctx, data: io.BytesIO=None):
    try:
        obj_client, namespace, bucket, main_file = initialize_process()
        x = load_sheets(namespace, bucket, main_file)
        x = first_transformation(x)
        x = second_transformation(x)
        x = third_transformation(x)
        x = fourth_transformation(x)
        x = fifth_transformation(x)
        x = sixth_transformation(x)
        x = extra_transformation(x)
        x = seventh_transformation(x)
        eight_transformation(x, obj_client)

        return response.Response(
            ctx, response_data=json.dumps(
                {"message": "Arquivos processados de parque atualizados no bucket TRUSTED com sucesso!"}),
            headers={"Content-Type": "application/json"}
        )
    except Exception as ex:
        return response.Response(
            ctx, response_data=json.dumps(
                {"message": "{0}".format(str(e))}),
            headers={"Content-Type": "application/json"}
    )