# 声明：本代码仅供学习和研究目的使用。使用者应遵守以下原则：  
# 1. 不得用于任何商业用途。  
# 2. 使用时应遵守目标平台的使用条款和robots.txt规则。  
# 3. 不得进行大规模爬取或对平台造成运营干扰。  
# 4. 应合理控制请求频率，避免给目标平台带来不必要的负担。   
# 5. 不得用于任何非法或不当的用途。
#   
# 详细许可条款请参阅项目根目录下的LICENSE文件。  
# 使用本代码即表示您同意遵守上述原则和LICENSE中的所有条款。  


# -*- coding: utf-8 -*-
# @Author  : relakkes@gmail.com
# @Time    : 2023/12/23 15:41
# @Desc    : 微博爬虫主流程代码


import asyncio
import os
import random
from asyncio import Task
from typing import Dict, List, Optional, Tuple

from playwright.async_api import (BrowserContext, BrowserType, Page,
                                  async_playwright)

import config
from base.base_crawler import AbstractCrawler
from proxy.proxy_ip_pool import IpInfoModel, create_ip_pool
from store import weibo as weibo_store
from tools import utils
from var import crawler_type_var, source_keyword_var

from .client import WeiboClient
from .exception import DataFetchError
from .field import SearchType
from .help import filter_search_result_card
from .login import WeiboLogin


class WeiboCrawler(AbstractCrawler):
    context_page: Page
    wb_client: WeiboClient
    browser_context: BrowserContext

    def __init__(self):  
        self.index_url = "https://www.weibo.com"  
        self.mobile_index_url = "https://m.weibo.cn"  
        self.user_agent = utils.get_user_agent()  
        self.mobile_user_agent = utils.get_mobile_user_agent()  
        self.checkpoint_dir = "checkpoints/weibo"  # 添加检查点目录

    async def start(self):
        playwright_proxy_format, httpx_proxy_format = None, None
        if config.ENABLE_IP_PROXY:
            ip_proxy_pool = await create_ip_pool(config.IP_PROXY_POOL_COUNT, enable_validate_ip=True)
            ip_proxy_info: IpInfoModel = await ip_proxy_pool.get_proxy()
            playwright_proxy_format, httpx_proxy_format = self.format_proxy_info(ip_proxy_info)

        async with async_playwright() as playwright:
            # Launch a browser context.
            chromium = playwright.chromium
            self.browser_context = await self.launch_browser(
                chromium,
                None,
                self.mobile_user_agent,
                headless=config.HEADLESS
            )
            # stealth.min.js is a js script to prevent the website from detecting the crawler.
            await self.browser_context.add_init_script(path="libs/stealth.min.js")
            self.context_page = await self.browser_context.new_page()
            await self.context_page.goto(self.mobile_index_url)

            # Create a client to interact with the xiaohongshu website.
            self.wb_client = await self.create_weibo_client(httpx_proxy_format)
            if not await self.wb_client.pong():
                login_obj = WeiboLogin(
                    login_type=config.LOGIN_TYPE,
                    login_phone="",  # your phone number
                    browser_context=self.browser_context,
                    context_page=self.context_page,
                    cookie_str=config.COOKIES
                )
                await login_obj.begin()

                # 登录成功后重定向到手机端的网站，再更新手机端登录成功的cookie
                utils.logger.info("[WeiboCrawler.start] redirect weibo mobile homepage and update cookies on mobile platform")
                await self.context_page.goto(self.mobile_index_url)
                await asyncio.sleep(2)
                await self.wb_client.update_cookies(browser_context=self.browser_context)

            crawler_type_var.set(config.CRAWLER_TYPE)
            if config.CRAWLER_TYPE == "search":
                # Search for video and retrieve their comment information.
                await self.search()
            elif config.CRAWLER_TYPE == "detail":
                # Get the information and comments of the specified post
                await self.get_specified_notes()
            elif config.CRAWLER_TYPE == "creator":
                # Get creator's information and their notes and comments
                await self.get_creators_and_notes()
            else:
                pass
            utils.logger.info("[WeiboCrawler.start] Weibo Crawler finished ...")
    
    def date_to_timestamp(self, date_str: str) -> int:  
        """  
        将YYYY-MM-DD格式的日期转换为时间戳  
        :param date_str: YYYY-MM-DD格式的日期字符串  
        :return: 时间戳（秒）  
        """  
        try:  
            from datetime import datetime  
            dt = datetime.strptime(date_str, '%Y-%m-%d')  
            return int(dt.timestamp())  
        except Exception as e:  
            utils.logger.error(f"[WeiboCrawler.date_to_timestamp] Error converting date: {date_str}, error: {e}")  
            return 0  
  
    def rfc2822_to_timestamp(self, time_str: str) -> int:  
            """  
            将微博返回的时间格式转换为时间戳  
            :param time_str: 微博返回的时间字符串，如 "Sat May 11 17:51:15 +0800 2024"  
            :return: 时间戳（秒）  
            """  
            try:  
                # 处理微博的时间格式  
                from email.utils import parsedate_to_datetime  
                dt = parsedate_to_datetime(time_str)  
                return int(dt.timestamp())  
            except Exception as e:  
                utils.logger.error(f"[WeiboCrawler.rfc2822_to_timestamp] Error converting time: {time_str}, error: {e}")  
                # 如果解析失败，返回当前时间戳  
                from datetime import datetime  
                return int(datetime.now().timestamp())

    async def search(self):  
        """  
        search weibo note with keywords  
        :return:  
        """  
        utils.logger.info("[WeiboCrawler.search] Begin search weibo keywords")  
        weibo_limit_count = 10  # weibo limit page fixed value  
        if config.CRAWLER_MAX_NOTES_COUNT < weibo_limit_count:  
            config.CRAWLER_MAX_NOTES_COUNT = weibo_limit_count  
        start_page = config.START_PAGE  
        
        # 转换日期为时间戳，用于精确过滤  
        start_timestamp = None  
        end_timestamp = None  
        if hasattr(config, 'START_DAY') and config.START_DAY:  
            start_timestamp = self.date_to_timestamp(config.START_DAY)  
        if hasattr(config, 'END_DAY') and config.END_DAY:  
            # 设置为当天结束时间  
            end_timestamp = self.date_to_timestamp(config.END_DAY) + 24*60*60 - 1  
        
        for keyword in config.KEYWORDS.split(","):  
            source_keyword_var.set(keyword)  
            utils.logger.info(f"[WeiboCrawler.search] Current search keyword: {keyword}")  
            
            # 不开启按天爬取  
            if not hasattr(config, 'ALL_DAY') or not config.ALL_DAY:  
                timescope = await self.get_timescope(  
                    getattr(config, 'START_DAY', None),   
                    getattr(config, 'END_DAY', None)  
                )  
                page = 1  
                valid_note_count = 0  # 记录有效的微博数量  
                empty_page_count = 0  # 记录连续空页数  
                
                while valid_note_count < config.CRAWLER_MAX_NOTES_COUNT and empty_page_count < 3:  
                    if page < start_page:  
                        utils.logger.info(f"[WeiboCrawler.search] Skip page: {page}")  
                        page += 1  
                        continue  
                        
                    utils.logger.info(f"[WeiboCrawler.search] search weibo keyword: {keyword}, page: {page}, timescope: {timescope}")  
                    search_res = await self.wb_client.get_note_by_keyword(  
                        keyword=keyword,  
                        page=page,  
                        search_type=SearchType.DEFAULT,  
                        timescope=timescope  
                    )  
                    
                    note_id_list: List[str] = []  
                    note_list = filter_search_result_card(search_res.get("cards"))  
                    
                    # 如果没有更多结果，退出循环  
                    if not note_list:  
                        utils.logger.info(f"[WeiboCrawler.search] No more results for keyword: {keyword}")  
                        break  
                    
                    page_valid_count = 0  # 当前页面符合日期条件的微博数  
                    
                    for note_item in note_list:  
                        if note_item:  
                            mblog: Dict = note_item.get("mblog")  
                            if mblog:  
                                # 检查微博创建时间是否在指定范围内  
                                create_time = self.rfc2822_to_timestamp(mblog.get("created_at"))  
                                
                                # 如果设置了时间范围，则进行过滤  
                                if (start_timestamp and create_time < start_timestamp) or \
                                (end_timestamp and create_time > end_timestamp):  
                                    utils.logger.info(f"[WeiboCrawler.search] Skip note outside time range: {mblog.get('id')}, create_time: {mblog.get('created_at')}")  
                                    continue  
                                    
                                note_id_list.append(mblog.get("id"))  
                                await weibo_store.update_weibo_note(note_item)  
                                await self.get_note_images(mblog)  
                                valid_note_count += 1  
                                page_valid_count += 1  
                                
                                # 如果达到了最大爬取数量，退出循环  
                                if valid_note_count >= config.CRAWLER_MAX_NOTES_COUNT:  
                                    break  
                    
                    # 处理评论  
                    await self.batch_get_notes_comments(note_id_list)  
                    
                    # 如果当前页没有符合条件的微博，增加空页计数  
                    if page_valid_count == 0:  
                        empty_page_count += 1  
                    else:  
                        empty_page_count = 0  # 重置空页计数  
                    
                    page += 1  
            # 开启按天爬取  
            else:  
                import pandas as pd  
                from datetime import datetime  
                
                start_day = getattr(config, 'START_DAY', datetime.now().strftime('%Y-%m-%d'))  
                end_day = getattr(config, 'END_DAY', datetime.now().strftime('%Y-%m-%d'))  
                
                for day in pd.date_range(start=start_day, end=end_day, freq='D'):  
                    day_str = day.strftime('%Y-%m-%d')  
                    day_start_timestamp = self.date_to_timestamp(day_str)  
                    day_end_timestamp = day_start_timestamp + 24*60*60 - 1  
                    
                    timescope = await self.get_timescope(day_str, day_str)  
                    page = 1  
                    valid_note_count = 0  
                    empty_page_count = 0  # 记录连续空页数  
                    
                    while valid_note_count < config.CRAWLER_MAX_NOTES_COUNT and empty_page_count < 3:  
                        try:  
                            utils.logger.info(f"[WeiboCrawler.search] search weibo keyword: {keyword}, date: {day_str}, page: {page}")  
                            search_res = await self.wb_client.get_note_by_keyword(  
                                keyword=keyword,  
                                page=page,  
                                search_type=SearchType.DEFAULT,  
                                timescope=timescope  
                            )  
                            
                            note_id_list: List[str] = []  
                            note_list = filter_search_result_card(search_res.get("cards"))  
                            
                            # 如果没有更多结果，退出循环  
                            if not note_list:  
                                utils.logger.info(f"[WeiboCrawler.search] No more results for keyword: {keyword} on date: {day_str}")  
                                break  
                            
                            page_valid_count = 0  # 当前页面符合日期条件的微博数  
                            
                            for note_item in note_list:  
                                if note_item:  
                                    mblog: Dict = note_item.get("mblog")  
                                    if mblog:  
                                        # 检查微博创建时间是否在当天范围内  
                                        create_time = self.rfc2822_to_timestamp(mblog.get("created_at"))  
                                        
                                        if create_time < day_start_timestamp or create_time > day_end_timestamp:  
                                            utils.logger.info(f"[WeiboCrawler.search] Skip note outside day range: {mblog.get('id')}, create_time: {mblog.get('created_at')}")  
                                            continue  
                                            
                                        note_id_list.append(mblog.get("id"))  
                                        await weibo_store.update_weibo_note(note_item)  
                                        await self.get_note_images(mblog)  
                                        valid_note_count += 1  
                                        page_valid_count += 1  
                            
                            # 处理评论  
                            await self.batch_get_notes_comments(note_id_list)  
                            
                            # 如果当前页没有符合条件的微博，增加空页计数  
                            if page_valid_count == 0:  
                                empty_page_count += 1  
                            else:  
                                empty_page_count = 0  # 重置空页计数  
                                
                            page += 1  
                            
                        except Exception as e:  
                            utils.logger.error(f"[WeiboCrawler.search] Error when crawling: {e}")  
                            break  # 发生错误跳到下一天

    async def get_note_info_task(self, note_id: str, semaphore: asyncio.Semaphore) -> Optional[Dict]:
        """
        Get note detail task
        :param note_id:
        :param semaphore:
        :return:
        """
        async with semaphore:
            try:
                result = await self.wb_client.get_note_info_by_id(note_id)
                return result
            except DataFetchError as ex:
                utils.logger.error(f"[WeiboCrawler.get_note_info_task] Get note detail error: {ex}")
                return None
            except KeyError as ex:
                utils.logger.error(
                    f"[WeiboCrawler.get_note_info_task] have not fund note detail note_id:{note_id}, err: {ex}")
                return None

    async def batch_get_notes_comments(self, note_id_list: List[str]):
        """
        batch get notes comments
        :param note_id_list:
        :return:
        """
        if not config.ENABLE_GET_COMMENTS:
            utils.logger.info(f"[WeiboCrawler.batch_get_note_comments] Crawling comment mode is not enabled")
            return

        utils.logger.info(f"[WeiboCrawler.batch_get_notes_comments] note ids:{note_id_list}")
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
        task_list: List[Task] = []
        for note_id in note_id_list:
            task = asyncio.create_task(self.get_note_comments(note_id, semaphore), name=note_id)
            task_list.append(task)
        await asyncio.gather(*task_list)

    async def get_note_comments(self, note_id: str, semaphore: asyncio.Semaphore):
        """
        get comment for note id
        :param note_id:
        :param semaphore:
        :return:
        """
        async with semaphore:
            try:
                utils.logger.info(f"[WeiboCrawler.get_note_comments] begin get note_id: {note_id} comments ...")
                await self.wb_client.get_note_all_comments(
                    note_id=note_id,
                    crawl_interval=random.randint(1,3), # 微博对API的限流比较严重，所以延时提高一些
                    callback=weibo_store.batch_update_weibo_note_comments,
                    max_count=config.CRAWLER_MAX_COMMENTS_COUNT_SINGLENOTES
                )
            except DataFetchError as ex:
                utils.logger.error(f"[WeiboCrawler.get_note_comments] get note_id: {note_id} comment error: {ex}")
            except Exception as e:
                utils.logger.error(f"[WeiboCrawler.get_note_comments] may be been blocked, err:{e}")

    async def get_note_images(self, mblog: Dict):
        """
        get note images
        :param mblog:
        :return:
        """
        if not config.ENABLE_GET_IMAGES:
            utils.logger.info(f"[WeiboCrawler.get_note_images] Crawling image mode is not enabled")
            return
        
        pics: Dict = mblog.get("pics")
        if not pics:
            return
        for pic in pics:
            url = pic.get("url")
            if not url:
                continue
            content = await self.wb_client.get_note_image(url)
            if content != None:
                extension_file_name = url.split(".")[-1]
                await weibo_store.update_weibo_note_image(pic["pid"], content, extension_file_name)


    async def get_creators_and_notes(self) -> None:
        """
        Get creator's information and their notes and comments
        Returns:

        """
        utils.logger.info("[WeiboCrawler.get_creators_and_notes] Begin get weibo creators")
        for user_id in config.WEIBO_CREATOR_ID_LIST:
            createor_info_res: Dict = await self.wb_client.get_creator_info_by_id(creator_id=user_id)
            if createor_info_res:
                createor_info: Dict = createor_info_res.get("userInfo", {})
                utils.logger.info(f"[WeiboCrawler.get_creators_and_notes] creator info: {createor_info}")
                if not createor_info:
                    raise DataFetchError("Get creator info error")
                await weibo_store.save_creator(user_id, user_info=createor_info)

                # Get all note information of the creator
                all_notes_list = await self.wb_client.get_all_notes_by_creator_id(
                    creator_id=user_id,
                    container_id=createor_info_res.get("lfid_container_id"),
                    crawl_interval=0,
                    callback=weibo_store.batch_update_weibo_notes
                )

                note_ids = [note_item.get("mblog", {}).get("id") for note_item in all_notes_list if
                            note_item.get("mblog", {}).get("id")]
                await self.batch_get_notes_comments(note_ids)

            else:
                utils.logger.error(
                    f"[WeiboCrawler.get_creators_and_notes] get creator info error, creator_id:{user_id}")



    async def create_weibo_client(self, httpx_proxy: Optional[str]) -> WeiboClient:
        """Create xhs client"""
        utils.logger.info("[WeiboCrawler.create_weibo_client] Begin create weibo API client ...")
        cookie_str, cookie_dict = utils.convert_cookies(await self.browser_context.cookies())
        weibo_client_obj = WeiboClient(
            proxies=httpx_proxy,
            headers={
                "User-Agent": utils.get_mobile_user_agent(),
                "Cookie": cookie_str,
                "Origin": "https://m.weibo.cn",
                "Referer": "https://m.weibo.cn",
                "Content-Type": "application/json;charset=UTF-8"
            },
            playwright_page=self.context_page,
            cookie_dict=cookie_dict,
        )
        return weibo_client_obj

    @staticmethod
    def format_proxy_info(ip_proxy_info: IpInfoModel) -> Tuple[Optional[Dict], Optional[Dict]]:
        """format proxy info for playwright and httpx"""
        playwright_proxy = {
            "server": f"{ip_proxy_info.protocol}{ip_proxy_info.ip}:{ip_proxy_info.port}",
            "username": ip_proxy_info.user,
            "password": ip_proxy_info.password,
        }
        httpx_proxy = {
            f"{ip_proxy_info.protocol}": f"http://{ip_proxy_info.user}:{ip_proxy_info.password}@{ip_proxy_info.ip}:{ip_proxy_info.port}"
        }
        return playwright_proxy, httpx_proxy

    async def launch_browser(
            self,
            chromium: BrowserType,
            playwright_proxy: Optional[Dict],
            user_agent: Optional[str],
            headless: bool = True
    ) -> BrowserContext:
        """Launch browser and create browser context"""
        utils.logger.info("[WeiboCrawler.launch_browser] Begin create browser context ...")
        if config.SAVE_LOGIN_STATE:
            user_data_dir = os.path.join(os.getcwd(), "browser_data",
                                         config.USER_DATA_DIR % config.PLATFORM)  # type: ignore
            browser_context = await chromium.launch_persistent_context(
                user_data_dir=user_data_dir,
                accept_downloads=True,
                headless=headless,
                proxy=playwright_proxy,  # type: ignore
                viewport={"width": 1920, "height": 1080},
                user_agent=user_agent
            )
            return browser_context
        else:
            browser = await chromium.launch(headless=headless, proxy=playwright_proxy)  # type: ignore
            browser_context = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=user_agent
            )
            return browser_context
        
    async def get_timescope(self, start: str = config.START_DAY, end: str = config.END_DAY) -> str:  
            """  
            获取微博搜索的时间范围参数  
            :param start: 开始日期，YYYY-MM-DD格式  
            :param end: 结束日期，YYYY-MM-DD格式  
            :return: 格式化的timescope参数  
            """  
            if not start or not end:  
                return None  
            return f"custom:{start}:{end}"
