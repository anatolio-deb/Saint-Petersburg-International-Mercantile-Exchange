from typing import Any, Coroutine, List
from bs4 import BeautifulSoup
import pathlib
import xlrd
from datetime import datetime
import asyncio
import aiohttp
import aiofiles
import logging
import matplotlib.pyplot as plt
import numpy as np


REPORTS_FOLDER = pathlib.Path("reports")
HTML_CACHE = pathlib.Path("cache")


def init():
    """
    Функция init подготавливает окружение (папку отчетов и кэша html
    cтраниц)
    """
    if not REPORTS_FOLDER.exists():
        REPORTS_FOLDER.mkdir()

    if not HTML_CACHE.exists():
        HTML_CACHE.mkdir()

    logging.basicConfig(
        format="%(asctime)s %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S %p",
        level=logging.INFO,
    )


async def get_html_page(
    url: str, param: str, count: int
) -> str | Coroutine[Any, Any, str]:
    """
    Корутина get_html_page считывает асинхронно и возвращает HTML страницу из
    кэша. Если HTML страница отсутствует в кэше, то выполняется асинхронный
    запрос к сайту, страница сохраняется в кэш. Возвращает содержимое страницы.
    """
    cache = HTML_CACHE / pathlib.Path(str(count) + ".html")

    if cache.exists():
        logging.info("loading from cache: %s", cache.as_posix())

        async with aiofiles.open(cache, "r") as outfile:
            return await outfile.read()

    params = None

    if param:
        params = {"page": param}

    sema = asyncio.BoundedSemaphore(5)

    async with sema, aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            assert response.status == 200

            logging.info("GET html page: %s", response.request_info.url)

            data = await response.text()

            async with aiofiles.open(cache, "w") as outfile:
                await outfile.write(data)

            return data


def get_html_elements(data: Coroutine[Any, Any, bytes], css_class: str):
    """
    Функция get_html_elements ищет и возвращает необходимые HTML элементы
    на странице по CSS классу css_class
    """
    soup = BeautifulSoup(data, "html.parser")
    elements = soup.findAll("div", {"class": css_class})
    return elements


def get_records():
    """
    Функция get_records формирует URL и запускает корутины, которые
    скачивают web страницу, сохраняя ее в кэш, ищут на странцие нужный
    HTNL элемент по его CSS классу, извлекают ссылку на скачивание .xls отчета,
    скачивают отчет, открывает его и ищут в нем необходимый товар и
    рыночную цену.

    Все данные сохраняются в виде струутур данных в памяти.
    Новыне корутины запускаются до первого скачанного отчета, который
    не содержит нужной информации.

    Возвращает структуру данных с датами и рыночными ценами.
    """
    records: List[dict] = []
    schema = "https://"
    domain = "spimex.com"
    url = f"{schema}{domain}/markets/oil_products/trades/results/"
    class_to_search = "accordeon-inner__item"
    text_to_search = "Бюллетень по итогам торгов в Секции «Нефтепродукты»"
    page_num = 0
    count = 0
    tasks = []
    loop = asyncio.get_event_loop()

    while True:
        count += 1

        if count > 1:
            page_num += 1
            param = f"page-{page_num}"
        else:
            param = ""

        async def download_and_analyze_report(url: str, param: str, count: int):
            target_col = "Рыночная"
            target_row = "A592UFM060F"

            data = await get_html_page(url, param, count)
            elements = get_html_elements(data, class_to_search)

            for element in elements:
                record: dict = {}
                tag = element.find("a", href=True)

                if text_to_search in tag:
                    date = element.find("span").text
                    path = REPORTS_FOLDER / pathlib.Path(date + ".xls")

                    if not path.exists():
                        link = tag["href"]
                        await download_xls(f"{schema}{domain}{link}", path)

                    price = get_market_price(path, target_row, target_col)

                    if not price:
                        path.unlink()
                        raise ValueError("market price is not found")

                    msg = f"market price for {target_row} found: {price}"
                    logging.info(msg)
                    record["date"] = datetime.strptime(date, "%d.%m.%Y")
                    record["price"] = price
                    record["path"] = path.absolute()
                    records.append(record)

        tasks.append(loop.create_task(download_and_analyze_report(url, param, count)))

        try:
            loop.run_until_complete(asyncio.gather(*tasks))
        except ValueError:
            break
    loop.close()

    return records


async def download_xls(link: str, path: pathlib.Path):
    """
    Корутина download_xls асинхронно скачивает .xls файл в директорию
    REPORTS_FOLDER и возвращает путь
    """
    sema = asyncio.BoundedSemaphore(5)

    async with sema, aiohttp.ClientSession() as session:
        async with session.get(link, allow_redirects=True) as response:
            assert response.status == 200
            logging.info("downloaded .xml file: %s", response.request_info.url)
            data = await response.read()

    async with aiofiles.open(path, "wb") as outfile:
        await outfile.write(data)


def get_market_price(path: pathlib.Path, rowname: str, colname: str) -> str:
    """
    Функция get_market_price считывает файл .xls и ищет в нем нужное
    занчение.
    """
    price: str = ""
    book = xlrd.open_workbook(path)
    sheet = book.sheet_by_index(0)
    colindex = None

    for i in range(sheet.ncols):
        if colname in sheet.col_values(i):
            colindex = i

    if colindex is not None:
        for i in range(sheet.nrows):
            if rowname in sheet.row_values(i):
                price = sheet.cell_value(i, colindex)

    return price


if __name__ == "__main__":
    prices = []
    dates = []
    init()
    records = get_records()

    for p in records:
        prices.append(p["price"])
        dates.append(p["date"])

    xaxis = np.array(dates)
    yaxis = np.array(prices)
    plt.plot(xaxis, yaxis)
    plt.show()
