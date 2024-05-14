import io
import json
from fdk import response
from helpers import initialize_process, load_sheets, first_transformation, second_transformation, third_transformation, fourth_transformation, fifth_transformation, sixth_transformation, extra_transformation, seventh_transformation, eight_transformation

def handler(ctx, data: io.BytesIO=None):
    try:
        print("> Iniciando processo...")
        obj_client, namespace, bucket, main_file = initialize_process()

        print("> Lendo planilhas")
        x = load_sheets(namespace, bucket, main_file)

        print("> 1a Transformação")
        x = first_transformation(x)

        print("> 2a Transformação")
        x = second_transformation(x)

        print("> 3a Transformação")
        x = third_transformation(x)

        print("> 4a Transformação")
        x = fourth_transformation(x)

        print("> 5a Transformação")
        x = fifth_transformation(x)

        print("> 6a Transformação")
        x = sixth_transformation(x)

        print("> Transformação Extra (problema com timestamp Spark e Pandas)")
        x = extra_transformation(x)

        print("> 7a Transformação")
        x = seventh_transformation(x)

        print("> Ùltima Transformação")
        eight_transformation(x, obj_client)

        print("# Processo finalizado com sucesso :D")
        return response.Response(
            ctx, response_data=json.dumps(
                {"message": "Arquivos processados de parque atualizados no bucket TRUSTED com sucesso!"}),
            headers={"Content-Type": "application/json"}
        )
    except Exception as ex:
        print("> Deu ruim :(, ", str(ex))
        return response.Response(
            ctx, response_data=json.dumps(
                {"message": "{0}".format(str(ex))}),
            headers={"Content-Type": "application/json"}
    )