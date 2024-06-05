import io
import json
from fdk import response
from helpers import set_initial_config, load_topdesk_data, load_tables, first_transformation, second_transformation, third_transformation, save_to_bucket

def handler(ctx, data: io.BytesIO=None):
    try:
        print("> Iniciando processo...")
        url, topdesk_auth, headers, obj_client  = set_initial_config()

        print("> Lendo dados do Topdesk...")
        dados = load_topdesk_data(url, topdesk_auth, headers)

        print("> Lendo tabelas do Topdesk")
        x = load_tables(topdesk_auth, url, headers, dados)

        print("> 1a Transformação")
        x = first_transformation(x)

        print("> 2a Transformação")
        x = second_transformation(x)

        print("> 3a Transformação")
        x = third_transformation(x)

        print("> Ùltima Transformação")
        save_to_bucket(obj_client, x)

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