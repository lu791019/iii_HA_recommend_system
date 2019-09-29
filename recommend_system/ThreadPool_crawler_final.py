from functools import wraps
import requests
from bs4 import BeautifulSoup
import re
import concurrent.futures
import time
import pandas as pd
import random 
import logging

LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(filename = "logs_of_crawler.txt",
                    level = logging.DEBUG,
                    format = LOG_FORMAT,
                    filemode = "a")
logger = logging.getLogger()


def my_timer(orig_func):
    "用來計時"
    import time
    @wraps(orig_func)
    def wrapper(*args, **kwargs):
        t1 = time.time()
        result = orig_func(*args, **kwargs)
        t2 = time.time() - t1
        print(f"{orig_func.__name__} ran in: {t2} sec.")
        return result
    return wrapper

@my_timer
def get_pages_url_list(category_idx: int) -> list:
    base_url = "https://www.everydayhealth.com.tw/category/" + str(category_idx) + "/index/"
    response = requests.get(base_url+"1")
    html_str = response.text
    soup = BeautifulSoup(html_str)

    final_page_url = soup.select(".disabled+ .disabled a")[0].get("href")
    pattern = re.compile(r'[0-9]+$')
    page = pattern.findall(final_page_url)[0]
    page_url_list = [base_url+str(i) for i in range(1, int(page)+1)]
    logger.debug("get_pages_url_list done.")
    return page_url_list

@my_timer
def get_articles_url_list(page_url: str) -> list:
    response = requests.get(page_url)
    html_str = response.text
    soup = BeautifulSoup(html_str)

    article_list = soup.select(".latest-articles-container .detail")
    article_url_list = []
    for article_url in article_list:
        article_url_list.append("https://www.everydayhealth.com.tw" + article_url.get("href"))
    logger.debug("get_articles_url_list done.")
    return article_url_list

@my_timer
def parse_article(article_url: str) -> list:
#     time.sleep(random.randint(0,4))
    response = requests.get(article_url)
    html_str = response.text
    soup = BeautifulSoup(html_str)
    need_crawl = {  "title": ".article .title",
                    "publication_date": ".date",
                    "author": ".autor",
                    "tags": ".tag-list",
                    "read_num": "span.number span",
                    "content": "#article_page"  }
    key_list = ["title", "publication_date", "author", "tags", "read_num", "content"]
    result = []
    for key in key_list:
        result.append(soup.select(need_crawl[key])[0].text)
    # 如果文章有多頁，再次呼叫此函式下載分頁文章，並將內容串接
    if soup.select("li+ li .actbtn") != []:
        next_page_url = soup.select("li+ li .actbtn")[0].get("href")
        if next_page_url is not None and next_page_url != "javascript:void(0)":
            next_page_result = parse_article(next_page_url)
            result[5] += next_page_result[5]
    # 正規表示法匹配URL最後的數字，單頁文章URL最後數字通常有五位數
    # 有多頁的文章最後數字為分頁數，通常都是個位數，設定99來判斷是否應該存檔
    if int(re.search("[0-9]*$", article_url).group(0)) > 99:
        result.append(article_url)
    return result


if __name__ == "__main__":
    t1 = time.time()
    today = "".join([str(time.localtime(time.time())[i]) for i in range(3)])
    c = ["title", "publication_date", "author", "tags", "read_num", "content", "article_url"]
    df_csv= pd.DataFrame(columns=column)
    with open(f"healthy_article_{today}.csv", "w", encoding="utf-8") as f:
        df_csv.to_csv(f, encoding="utf-8", index=False)
    task_list = []
    
    out_list = []
    for i in [3, 51, 46, 5, 48, 54, 4, 55]:
        out_list.extend(get_pages_url_list(i))
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        for i in executor.map(get_articles_url_list, out_list):
            task_list.extend(i)
            
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = executor.map(parse_article, task_list)
        try:
            for r in results:
                s = pd.Series(r, index=c)
                df_csv = df_csv.append(s, ignore_index=True)
        except:
            print("hey~")
        with open(f"healthy_article_{today}.csv", "a", encoding="utf-8") as f:
            df_csv.to_csv(f, encoding="utf-8", index=False, header=False)
        
    t2 = time.time()
    print(f"start:{time.asctime(time.localtime(t1))} end:{time.asctime(time.localtime(time.time()))} total ran in: {t2 - t1} sec.")
