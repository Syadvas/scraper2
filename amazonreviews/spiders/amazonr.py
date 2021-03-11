import scrapy
import re
import time
from scrapy import Request
import logging
import json
from bs4 import BeautifulSoup
from lxml import etree
from scrapy.selector import Selector
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from lxml.etree import tostring
import pandas as pd
from nordvpn_switcher import initialize_VPN,rotate_VPN,terminate_VPN



class AmazonrSpider(scrapy.Spider):
    name = 'amazonr'
    allowed_domains = ['*']
    start_urls = ['http://amazon.com/']
    df = pd.read_csv(r'C:\Amazon Reviews scraper(part1)\Scraper\reviewsScraper\cleanedProducts.csv')
    link_asin = list(zip(df.SeeAllReviews,df.asin))[500:1500]
    DRIVER_PATH = r"E:\ChromeDriver\chromedriver.exe"
    driver = webdriver.Chrome(executable_path=DRIVER_PATH)
    counter = 0
    settings = initialize_VPN(save=1,area_input=['complete rotation'])
    rotate_VPN(settings)

    def start_requests(self):
        for pair in self.link_asin:
            self.driver.get(pair[0])
            request = Request(self.driver.current_url,callback=self.parseReview,dont_filter=True, meta={'item': pair[1]})
            yield request

    def parseReview(self, response):
        asin = response.meta['item']
        self.driver.get(response.url)
        try:
            element = WebDriverWait(self.driver, 5).until(
            EC.presence_of_element_located((By.XPATH, '//*[@class="a-section a-spacing-none review-views celwidget"]//*[@data-hook="review-star-rating"]'))
            )
            #scrolling to element
            time.sleep(1)
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            #create xml tree to use xpath
            dom = etree.HTML(str(soup))

            reviewTitle = dom.xpath('//*[@class="a-section a-spacing-none review-views celwidget"]//*[@data-hook="review-title"]//text()')
            reviewTitle = [i.strip() for i in reviewTitle if i.strip() != '' ]
            reviewRatings = dom.xpath('//*[@class="a-section a-spacing-none review-views celwidget"]//*[@data-hook="review-star-rating"]//text()')
            
            reviewText = dom.xpath('//*[@class="a-section a-spacing-none review-views celwidget"]//*[@data-hook="review-body"]//text()')
            reviewText = [i.strip() for i in reviewText if i.strip() != '']
            reviewDate = dom.xpath('//*[@data-hook="review-date"]/text()')

            all_v = list(zip(reviewTitle,reviewText,reviewRatings,reviewDate))
            for i in all_v:
                rtitle = i[0]
                rtext = i[1]
                rrating = i[2]
                rdate = i[3]
                dict_ = {"asin":asin, "reviewTitle":rtitle,"reviewText":rtext,"reviewRatings":rrating,"reviewDate":rdate}
                with open("Reviews.json","a") as fl:
                    json.dump(dict_,fl)
                    fl.write('\n')
            
            nextpage = dom.xpath('//*[@class="a-last"]//@href') # nextpage link


            self.counter = self.counter + 1

            if self.counter%200 == 0 : # to rotate proxy after every 250 rotations
                print('!@#$%^&*()!@##$$%!@#$%^&*(!@#$%^&*()!@##$$%!@#$%^&*(!@#$%^&*()!@##$$%!@#$%^&*(')
                settings = initialize_VPN(save=1,area_input=['complete rotation'])
                rotate_VPN(settings)

            if str(nextpage) != '[]': # go to next page
                nextpage = "https://www.amazon.com" + nextpage[0]
            
                yield Request(nextpage,callback=self.parseReview,dont_filter=True,meta={'item': asin})
            else:
                pass
        except:
            pass