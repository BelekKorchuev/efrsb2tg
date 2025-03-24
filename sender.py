import requests
from bs4 import BeautifulSoup
from pprint import pprint

CATEGORIES = {
    "Транспорт": [
        "автомобиль", "машина", "грузовик", "коммерческий транспорт",
        "автобус", "спецтехника", "прицеп", "водный транспорт",
        "катер", "яхта", "лодка", "авиатехника", "самолет",
        "вертолет", "мотоцикл", "мототехника", "скутер"
    ],
    "Недвижимость": [
        "жилая недвижимость", "нежилая недвижимость", "квартира", "комната",
        "дом", "участок", "земельный участок", "гараж", "машино-место",
        "помещение", "офис", "здание"
    ],
    "Производство и электроника": [
        "оборудование", "станок", "производственное помещение", "промышленное",
        "компьютер", "оргтехника", "электроника", "инструмент", "станки",
        "производство", "софт", "имущественный комплекс"
    ],
    "Задолженности и ценные бумаги": [
        "долг", "задолженность", "ценные бумаги", "акции", "облигации",
        "права требования"
    ],
    "Торговое оборудование и ТМЦ": [
        "торговое оборудование", "тмц", "мебель", "драгоценности",
        "ювелирные изделия", "товарно-материальные ценности"
    ],
    "Сельское хозяйство": [
        "сельское хозяйство", "сельхоз", "трактор", "комбайн", "ферма",
        "сельхозтехника"
    ]
}

def clean_text(text):
    """Удаляет лишние символы из текста и приводит его в читаемый вид"""
    text = text.replace('\xa0', ' ').replace('\t', ' ').strip()
    # Убираем лишние пробелы, если они есть
    text = " ".join(text.split())
    return text


def determine_classification(description):
    """
    Пример простой эвристики для определения классификации на основе ключевых слов в описании.
    Можешь расширять или модифицировать словарь по мере необходимости.
    """
    desc_lower = description.lower()
    for category, keywords in CATEGORIES.items():
        for keyword in keywords:
            if keyword in desc_lower:
                return category
    return "Не определена"

def link_parser(link):
    response = requests.post(link)
    soup = BeautifulSoup(response.text, 'html.parser')

    data = {
        'Ссылка': link
    }

    messages = []

    # Извлечение заголовка сообщения
    title = soup.find('h1', class_='red_small').text.strip()


    table_headinfo = soup.find('table', class_='headInfo')
    if table_headinfo:
        raws = table_headinfo.find_all('tr')
        for raw in raws:
            cells = raw.find_all('td')
            if len(cells) == 2:
                field = cells[0].text.strip()
                value = cells[1].text.strip()
                if "Дата публикации" in field:
                    data['Дата публикации'] = value


    # Данные о должнике
    debtor_section = soup.find('div', string="Должник")
    if debtor_section:
        debtor_table = debtor_section.find_next('table')
        if debtor_table:
            debtor_rows = debtor_table.find_all('tr')
            for row in debtor_rows:
                cells = row.find_all('td')
                if len(cells) == 2:
                    field = cells[0].text.strip()
                    value = cells[1].text.strip()
                    data[field] = clean_text(value)

    # Информация об арбитражном управляющем
    arbiter_section = soup.find('div', string="Кем опубликовано")
    if arbiter_section:
        arbiter_table = arbiter_section.find_next('table')
        if arbiter_table:
            arbiter_rows = arbiter_table.find_all('tr')
            for row in arbiter_rows:
                cells = row.find_all('td')
                if len(cells) == 2:
                    field = cells[0].text.strip()
                    value = cells[1].text.strip()
                    data[field] = clean_text(value)

    if "Объявление о проведении торгов" in title:
        info_section = soup.find('div', string="Публикуемые сведения")
        if info_section:
            info_table = info_section.find_next('table')
            if info_table:
                info_rows = info_table.find_all('tr')
                for row in info_rows:
                    cells = row.find_all('td')
                    if len(cells) == 2:
                        field = cells[0].text.strip()
                        value = cells[1].text.strip()

                        if "Вид торгов" in field:
                            data[field] = value

        table_lotinfo = soup.find('table', class_='lotInfo')
        if table_lotinfo:
            rows = table_lotinfo.find_all('tr')
            headers = [th.text.strip() for th in table_lotinfo.find_all("th")]
            for row in rows[1:]:
                lot_row = [td.text.strip() for td in row.find_all("td")]  # Извлекаем текст ячеек
                lot_dict = dict(zip(headers, lot_row))
                lot = data.copy()
                lot.update({
                    "Описание": lot_dict["Описание"].strip(),
                    "Классификация": lot_dict["Классификация имущества"].strip(),
                    "Цена": lot_dict["Начальная цена, руб"].strip()
                })
                messages.append(lot)

    if "Отчет оценщика об оценке имущества должника" in title:
        lots = []

        lot_section = soup.find('div', string="Сведения об объектах оценки")
        if lot_section:
            lot_table = lot_section.find_next("table")
            if lot_table:
                lot_rows = lot_table.find_all('tr')
                headers = [th.text.strip() for th in lot_table.find_all("th")]

                for row in lot_rows[1:]:
                    lot_row = [td.text.strip() for td in row.find_all("td")]  # Извлекаем текст ячеек
                    lot_dict = dict(zip(headers, lot_row))
                    lot = data.copy()
                    lot.update({
                        "Описание": lot_dict["Описание"].strip(),
                        "Классификация": lot_dict["Тип"].strip(),
                        "Цена": lot_dict["Стоимость,определеннаяоценщиком"].strip()
                    })
                    messages.append(lot)

    pprint(messages)
    return messages
