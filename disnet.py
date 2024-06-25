import os
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from termcolor import colored

from dissernet_module.date_converter import convert
from dissernet_module.lexicon import (
    lex_date,
    lex_dissovet,
    lex_opponents,
    lex_sci_adviser,
    load_menu,
    main_menu,
)
from dissernet_module.load_db import (
    get_active_dissertants,
    get_by_status,
    load_row,
    update_changelog,
    update_csv,
)

load_dotenv()


MAIN_PAGE_URL = "https://www.dissernet.org"
# Минимальное количество дней для проверки статуса пользователей
# категории 'new' с последней проверки
REFRESH_NEW_FREQUENCY = os.getenv("REFRESH_NEW_FREQUENCY")
# Минимальное количество дней для проверки статуса пользователей
# категории 'new' с последней проверки
REFRESH_CURRENT_FREQUENCY = os.getenv("REFRESH_CURRENT_FREQUENCY")


def smart_print(output, source=None, color=None):
    time.sleep(0.5)
    if source:
        return f"<p style='color:{color}'>{output}</p>"
    else:
        print(colored(output, color))


def get_date() -> str:
    """Получает сегодняшнюю дату"""
    return time.strftime("%d.%m.%Y", time.localtime())


def download_html(URL) -> str:
    """Загружает html страницу указанного url"""
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0"
    }
    request = requests.get(URL, headers=headers)
    return request.text


def downlod_html_from_file(filename) -> str:
    """Загружает html странцу из указанного файла"""
    with open(filename, "r") as f:
        page = f.read()
    return page


def get_defences(html, df_active, df_expired):
    """Возвращает список url защит организации"""
    soup = BeautifulSoup(html, "html.parser")
    defences = [
        link.get("href")
        for link in soup.find_all(href=re.compile("expertise/"))
    ]
    expired = get_by_status(["expired"], df_active, df_expired)["Слаг"]
    # удаление диссертантов с просроченным сроком из списка
    defences = [
        defence
        for defence in defences
        if (defence.split("/")[-1] not in set(expired.tolist()))
    ]
    return defences


def save_html(html, path, filename):
    """Сохраняет html в папку path"""
    with open(f"{path}\{filename}.html", "w") as file:
        file.write(html)


def get_mainpages(MAIN_PAGE_PATH):
    """Возвращает список файлов папки mainpages"""
    files = os.listdir(MAIN_PAGE_PATH)
    if not files:
        raise Exception("папка mainpages пуста")
    print("выберите номер файла")
    enum_files = {(num + 1): filename for num, filename in enumerate(files)}
    for file_num, file in enum_files.items():
        print(f"{file_num}) {file}")
    user_choice = int(input())
    try:
        return downlod_html_from_file(
            f"{MAIN_PAGE_PATH}\{enum_files[user_choice]}"
        )
    except Exception:
        yield smart_print("Ошибка при открытии файла")


def get_status(date, is_zolus):
    """Определяет статус"""
    if ((datetime.now() - convert(date)).days // 365) > 11:
        return "expired"
    if is_zolus:
        return "current"
    else:
        return "new"


def get_revoke_info(soup):
    """Получает статус, дату и дс о лишении степени"""
    time.sleep(1)
    revoke_link = MAIN_PAGE_URL + soup.find(
        href=re.compile("revocation-of-degrees/")
    ).get("href")
    revoke_html = download_html(revoke_link)
    soup = BeautifulSoup(revoke_html, "html.parser")
    status = soup.find(
        string=re.compile("Статус")
    ).parent.parent.next_sibling.next_sibling.text
    status_revoke = status.strip() if status else ""
    date_revoke = soup.find(
        string=re.compile("Дата подачи заявления")
    ).parent.parent.next_sibling.next_sibling.text
    date_revoke = date_revoke.strip() if date_revoke else ""
    cypher = soup.find(string=re.compile("(жалоба)"))
    if cypher:
        cypher = cypher.parent.parent.next_sibling.next_sibling.text
    cypher = cypher.strip() if cypher else ""
    conclusion = soup.find(string=re.compile("Решение диссовета"))
    if conclusion:
        conclusion = conclusion.parent.parent.next_sibling.next_sibling.text
        conclusion = re.sub(r"\s+", " ", conclusion)
    else:
        conclusion = ""
    return (status_revoke, date_revoke, cypher, conclusion)


def b_tag_finder(soup):
    """Возвращает список объектов <b> со страницы"""
    b_tags = soup.find_all("b")
    data_dict = {str(tag): tag.parent.next_sibling for tag in b_tags}
    return data_dict


def is_mismatch(slug, data_for_comparison, ACTIVE_DISSERTANTS):
    """Выявляет различие в данных с сайта и с бд"""
    mismatch = []
    dissertant = ACTIVE_DISSERTANTS[slug]
    for field in [
        "Статус",
        "Подан Золус",
        "Дата подачи ЗоЛУС",
        "Шифр диссовета (жалоба)",
        "Решение диссовета",
    ]:
        if data_for_comparison[field] != dissertant[field]:
            mismatch.append(field)
    return mismatch


def needs_update(slug, refresh_new, refresh_cur, ACTIVE_DISSERTANTS):
    dissertant = ACTIVE_DISSERTANTS[slug]
    update_date = datetime.strptime(dissertant["Дата обновления"], "%d.%m.%Y")
    today = datetime.strptime(get_date(), "%d.%m.%Y")
    days_delta = (today - update_date).days
    if dissertant["Статус"] == "new":
        if days_delta >= int(refresh_new):
            return True
    if dissertant["Статус"] == "current":
        if days_delta >= int(refresh_cur):
            return True
    return False


def save_defences(
    defence_html,
    slug,
    source=None,
    ACTIVE_DISSERTANTS=None,
    ACTIVE_PATH=None,
    EXPIRED_PATH=None,
):
    """Формирует информацию со страницы защиты и сохраняет в бд"""
    try:
        update_info = []
        soup = BeautifulSoup(defence_html, "html.parser")
        b_tags_parents = b_tag_finder(soup)
        dissername = soup.h2.get_text()
        dissertant = soup.find(href=re.compile("person/")).text
        sci_adviser = ", ".join(
            [
                tag.text
                for tag in b_tags_parents[
                    lex_sci_adviser
                ].next_sibling.find_all("a")
            ]
        )
        opponents = ", ".join(
            [
                tag.text
                for tag in b_tags_parents[lex_opponents].next_sibling.find_all(
                    "a"
                )
            ]
        )
        dissovet = (
            b_tags_parents[lex_dissovet].next_sibling.find("a").text.strip()
        )
        is_zolus = bool(soup.find(string=re.compile("заявление о лишении")))
        (status, status_revoke, date_of_revoke, cypher, conclusion) = (
            "",
            "",
            "",
            "",
            "",
        )
        if is_zolus:
            (
                status_revoke,
                date_of_revoke,
                cypher,
                conclusion,
            ) = get_revoke_info(soup)
        date = b_tags_parents[lex_date].next_sibling.span.text.lower()
        status = get_status(date, is_zolus)
        # информация в виде словаря для сверки данных
        # со словарем из бд
        data_for_comparison = {
            "Статус": status,
            "Подан Золус": is_zolus,
            "Дата подачи ЗоЛУС": date_of_revoke,
            "Шифр диссовета (жалоба)": cypher,
            "Решение диссовета": conclusion,
        }
        data_ls = [
            get_date(),
            slug,
            dissertant,
            status,
            dissername,
            sci_adviser,
            opponents,
            dissovet,
            date,
            is_zolus,
            status_revoke,
            date_of_revoke,
            cypher,
            conclusion,
        ]
        time.sleep(1)
        if status == "expired":
            output = (
                "Выявлен новый диссертант вышедшей из срока\n"
                "подачи заявления на лишение степени"
            )
            load_row(EXPIRED_PATH, data_ls)
            output = smart_print(output, source=source, color="red")
            return (None, output)
        if slug in ACTIVE_DISSERTANTS:
            mismatch = is_mismatch(
                slug, data_for_comparison, ACTIVE_DISSERTANTS
            )
            if mismatch:
                result = ", ".join(mismatch)
                output = f"Обновление по {slug}: {result}"
                update_info = result
                output = smart_print(output, source=source, color="purple")
                update_changelog(slug, result, get_date())
            else:
                output = "Обновлений нет"
                output = smart_print(output, source=source, color="green")
        else:
            output = f"В базу внесен новый диссертант {dissertant}"
            output = smart_print(output, source=source, color="red")
        return (data_ls, output, update_info)
    except Exception as err:
        output = f"ошибка при обработке {slug}: {err}"
        return smart_print(output, source=source, color="red")


def parse_dissernet(
    df_active,
    df_expired,
    source,
    ACTIVE_PATH=None,
    EXPIRED_PATH=None,
    refresh_new=REFRESH_NEW_FREQUENCY,
    refresh_cur=REFRESH_CURRENT_FREQUENCY,
    MAIN_PAGE_PATH=Path(Path.cwd(), "ВМедА", "mainpages"),
    DEFENCES_PAGE_PATH=Path(Path.cwd(), "ВМедА", "defences"),
    DEFENCES_URL=None,
):
    ACTIVE_DISSERTANTS = get_active_dissertants(df_active)
    ACTIVE_PATH = ACTIVE_PATH if ACTIVE_PATH else "ВМеДА/active.csv"
    EXPIRED_PATH = EXPIRED_PATH if EXPIRED_PATH else "ВМеДА/expired.csv"
    update_list = []
    count = 0
    datalist = []
    html = download_html(DEFENCES_URL)
    defences = get_defences(html, df_active, df_expired)
    save_html(html, MAIN_PAGE_PATH, get_date())
    for defence in defences:
        count += 1
        time.sleep(1)
        url = MAIN_PAGE_URL + defence
        time.sleep(1)
        slug = defence.split("/")[-1].split(".")[0]
        if slug in ACTIVE_DISSERTANTS:
            yield smart_print(
                f"Проверяется {count} диссертант "
                f"{ACTIVE_DISSERTANTS[slug]['Диссертант']}",
                source=source,
                color="black",
            )
            if (
                "лишить степени"
                in ACTIVE_DISSERTANTS[slug]["Решение диссовета"]
            ):
                data_ls = ACTIVE_DISSERTANTS[slug].values()
                yield smart_print(
                    "- решение по диссертации вынесено,"
                    " загрузка данных из бд...",
                    source=source,
                    color="black",
                )
            elif needs_update(
                slug, refresh_new, refresh_cur, ACTIVE_DISSERTANTS
            ):
                yield smart_print(
                    "- обновление информации с dissernet...",
                    source=source,
                    color="black",
                )
                defence_html = download_html(url)
                save_html(defence_html, DEFENCES_PAGE_PATH, slug)
                data_ls, output, update_info = save_defences(
                    defence_html,
                    slug,
                    source,
                    ACTIVE_DISSERTANTS,
                    ACTIVE_PATH=ACTIVE_PATH,
                    EXPIRED_PATH=EXPIRED_PATH,
                )

                yield output
                if output != "<p style='color:green'>Обновлений нет</p>":
                    update_list.append(
                        f"{ACTIVE_DISSERTANTS[slug]['Диссертант']}: {update_info}"
                    )
            else:
                yield smart_print(
                    "- срок последнего обновления не превышает"
                    " контрольное значение, загрузка из бд...",
                    source=source,
                    color="black",
                )
                data_ls = ACTIVE_DISSERTANTS[slug].values()
        else:
            defence_html = download_html(url)
            save_html(defence_html, DEFENCES_PAGE_PATH, slug)
            data_ls, output, update_info = save_defences(
                defence_html,
                slug,
                source,
                ACTIVE_DISSERTANTS,
                ACTIVE_PATH=ACTIVE_PATH,
                EXPIRED_PATH=EXPIRED_PATH,
            )
            yield output
            update_changelog(slug, "Новый диссертант", get_date())
        if data_ls:
            datalist.append(data_ls)
        time.sleep(1)
    if datalist:
        update_csv(ACTIVE_PATH, datalist, "w", True)
    if update_list:
        report = smart_print(
            "НАЙДЕНЫ ОБНОВЛЕНИЯ:\n", color="red", source="mock"
        ) + ", ".join(update_list)
        yield report
    else:
        report = (
            smart_print(
                "|-----------------------------------------|\n",
                color="blue",
                source="mock",
            )
            + smart_print(
                "|ВАЖНЫЕ ОБНОВЛЕНИЯ НЕ НАЙДЕНЫ|\n", color="blue", source="mock"
            )
            + smart_print(
                "|-----------------------------------------|",
                color="blue",
                source="mock",
            )
        )
        yield report


def main():
    ans = int(input(main_menu))
    df_active, df_expired = pd.read_csv("ВМеда/active.csv"), pd.read_csv(
        "ВМеда/expired.csv"
    )
    match ans:
        case 1:
            output_to_status = {"1": "expired", "2": "new", "3": "current"}
            user_out = input(load_menu)
            if user_out in output_to_status:
                user_choice = output_to_status[user_out]
                if user_choice != "current":
                    print(
                        get_by_status([user_choice], df_active, df_expired)[
                            ["Диссертант", "Статус"]
                        ]
                    )
                else:
                    print(
                        get_by_status([user_choice], df_active, df_expired)[
                            [
                                "Диссертант",
                                "Статус ЗоЛУС",
                                "Дата подачи ЗоЛУС",
                                "Шифр диссовета (жалоба)",
                                "Решение диссовета",
                            ]
                        ]
                    )
                main()
            else:
                print("Некорректный ввод данных")
        case 3:
            exit()
        case 2:
            for out in parse_dissernet(
                pd.read_csv("ВМедА/active.csv"),
                pd.read_csv("ВМедА/expired.csv"),
                None,
                DEFENCES_URL="https://www.dissernet.org/organization/voyenno_meditsinskaya_akademiya_im_s_m_kirova?key=14&a[perPage]=all&main[perPage]=all",
            ):
                print(out)


if __name__ == "__main__":
    main()
