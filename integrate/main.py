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
        print('extraindo pagina %d' % page)
        try:
            response = requests.get(
                "http://127.0.0.1:5001/extract?page=" + str(page))
            json_content = json.loads(response.content)
            return json_content
        except:
            f = open("failed_pages.txt", "a+")
            f.write(str(page) + '\n')
            f.close()
            raise Exception('falha ao extrair dados da pagina %d' % page)

    def on_success(self, result):
        still_have_data = result["stillHaveData"]

        if still_have_data:
            print('pagina %d extraida com sucesso' %
                  int(result["currentPage"]))
        else:
            print('pagina %d não possui dados' % int(result["currentPage"]))

    def on_fail(self, exception):
        print(exception)

    def do_extract(self, pages):
        with concurrent.futures.ProcessPoolExecutor(pack_size) as executor:
            rx.from_(pages).pipe(
                ops.flat_map(lambda s: executor.submit(self.extract, s)),
            ).subscribe(
                self.on_success,
                self.on_fail)


def retry_failed_pages():
    if os.path.isfile('failed_pages.txt'):
        print('paginas que falharam: ')

        with open('failed_pages.txt') as f:
            lines = f.read().splitlines()
        print(lines)

        print('extraindo novamente as paginas que falharam')
        os.remove("failed_pages.txt")

        # map transforma os dados da lista em inteiros
        Extract().do_extract(list(map(int, lines)))


while still_have_data:
    Extract().do_extract(list(range(current_page, current_page + pack_size)))

    if current_page > 900:
        still_have_data = False

    current_page = current_page + pack_size

retry_failed_pages()
