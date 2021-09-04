import concurrent.futures
import time

import rx
from rx import operators as ops

PACK_SIZE = 100


def extract(page):
    # TODO executar o extract passando a pagina(tm)
    print('extraindo pagina %d' % page)
    time.sleep(1)
    return page


def output(result):
    print('page %d extraida' % result)


current_page = 1

with concurrent.futures.ProcessPoolExecutor(10) as executor:

    pages = list(range(current_page, current_page + PACK_SIZE))

    rx.from_(pages).pipe(
        ops.flat_map(lambda s: executor.submit(extract, s))
    ).subscribe(output)
