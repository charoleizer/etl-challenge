import concurrent.futures
import time
import os

import json
import requests

import rx
from rx import operators as ops

pack_size = 100
current_page = 1
still_have_data = True


class Extract:
    def extract(self, page):
        response = requests.get(
            "http://127.0.0.1:5001/extract?page=" + str(page))

        # Ao retornar 200 significa que o servico de extração conseguiu responder (não confundir com retorno da API consumida pelo serviço de extração)
        if response.status_code == 200:
            json_content = json.loads(response.content)
        else:
            json_content = {'statusOk': False}

        if json_content['statusOk']:
            return json_content
        else:
            f = open("failed_pages.txt", "a+")
            f.write(str(page) + '\n')
            f.close()

    def on_success(self, result):
        if (result) and (not result['stillHaveData']) and (not os.path.isfile('still_have_data.txt')):
            f = open("still_have_data.txt", "w+")
            f.write('done')
            f.close()

    def do_extract(self, pages):
        with concurrent.futures.ProcessPoolExecutor(pack_size) as executor:
            rx.from_(pages).pipe(
                ops.flat_map(lambda s: executor.submit(self.extract, s)),
            ).subscribe(
                self.on_success)


def retry_failed_pages():
    print('paginas que falharam: ')
    time.sleep(5)

    f = open("failed_pages.txt")
    lines = f.read().splitlines()
    f.close()
    print(lines)

    print('extraindo novamente as paginas que falharam')

    os.remove("failed_pages.txt")
    # map transforma os dados da lista em inteiros
    Extract().do_extract(list(map(int, lines)))

    if os.path.isfile('failed_pages.txt'):
        retry_failed_pages()


# Inicio da extração Normal
while still_have_data:
    Extract().do_extract(list(range(current_page, current_page + pack_size)))
    still_have_data = not os.path.isfile('still_have_data.txt')

    if current_page > 9000:
        still_have_data = False

    current_page = current_page + pack_size

# Caso houver falhas nas extrações, esse processo ficara em recurção até conseguir extrair todas as falhas
if os.path.isfile('failed_pages.txt'):
    retry_failed_pages()

if os.path.isfile('still_have_data.txt'):
    os.remove("still_have_data.txt")
