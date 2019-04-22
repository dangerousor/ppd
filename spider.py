#!/usr/bin/python
# -*- coding:utf-8 -*-
import base64
import json
import time

import requests

from db import Loan, DBWorker, User, Statistic, Record, DebtRecord, PreviousListing
from rd import r, r_ip
from ip import Ip
from users import user


class Spider:
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36',
    }
    header2 = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36',
        'Content-Type': 'application/json;charset=UTF-8',
    }
    session = requests.Session()
    dbWorker = DBWorker()

    def __init__(self):
        self.login()
        pass

    def get_html(self, url, header=None):
        if not header:
            header = self.header
        res = self.session.get(url, headers=header, timeout=30)
        if res.status_code != 200:
            print(res.content.decode())
            exit(res.status_code)
        return res

    def post_html(self, url, data=None, header=None):
        if not header:
            header = self.header
        res = self.session.post(url, data=data, headers=header)
        if res.status_code != 200:
            if res.status_code == 502:
                print(502)
                time.sleep(180)
                return self.post_html(url, data, header)
            print(res.status_code)
            print(res.content.decode())
            exit(res.status_code)
        return res

    def confirm_login(self, token):
        return self.session.post('https://tz.ppdai.com/api/raptor/h5api/userDetailInfoV3', headers=self.header2, data=json.dumps({'token': token}))

    def login(self):
        html = self.get_html('https://ac.ppdai.com/ValidateCode/Image')
        base64_data = base64.b64encode(html.content)
        captcha = 'data:image/jpeg;base64,' + base64_data.decode()
        header_captcha = {
            'appCode': '097CF01DC1CBCBAE95BEF83FFFFA9B29',
            'appKey': 'AKID56N045Os4eKLH0qT7Jb9gt37utWg2y4ONx1B',
            'appSecret': '64iswEpdq6ad9SoQ715fHnyk7z47Fa7Ogtf8ST93',
        }
        data_captcha = {
            'v_pic': captcha,
            'v_type': 'n4',
        }
        res = requests.post('http://apigateway.jianjiaoshuju.com/api/v_1/yzm.html', headers=header_captcha, data=data_captcha)
        if res.json()['errCode'] != 0:
            print(res.json())
            exit(1)
        url = 'https://ac.ppdai.com/User/Login'
        data = {
            'IsAsync': 'true',
            'Redirect': '',
            'UserName': user['username'],
            'Password': user['password'],
            'ValidateCode': res.json()['v_code'],
            'RememberMe': 'true',
        }
        html = self.post_html(url, data).json()
        if html['Code'] == 1:
            return
        else:
            if html['Message'] == '验证码输入有误':
                print('验证码输入有误')
                self.login()
            else:
                if html['Message'] == '您的帐号登录频繁，请稍后再试':
                    print('您的帐号登录频繁，请稍后再试')
                    time.sleep(180)
                    self.login()
                else:
                    print(html)
                    exit(-1)
        # ck_dict = requests.utils.dict_from_cookiejar(self.session.cookies)
        # res = self.confirm_login(ck_dict['token'])
        # print(res.content.decode())

    def step0(self):
        ip = Ip()
        data = {
            'authInfo': "",
            'authenticated': False,
            'availableBalance': 0,
            'creditCodes': "",
            'dataList': [],
            'didIBid': "0",
            'maxAmount': 0,
            'minAmount': 0,
            'months': "",
            'needTotalCount': True,
            'pageCount': 0,
            'pageIndex': 1,
            'pageSize': 10,
            'rates': "",
            'riskLevelCategory': 0,
            'sort': 0,
            'source': 1,
            'successLoanNum': "",
            'totalCount': 0,
        }
        while True:
            if r_ip.dbsize() < 3:
                ip.get_ips()
            print(str(data['riskLevelCategory']) + '---------' + str(data['pageIndex']))
            # 代理服务器
            proxy_host = r_ip.randomkey().decode()
            proxy_port = r_ip.get(proxy_host).decode()
            proxy_meta = "http://%(host)s:%(port)s" % {
                "host": proxy_host,
                "port": proxy_port,
            }
            proxies = {
                "http": proxy_meta,
                "https": proxy_meta,
            }
            # res = json.loads(self.post_html('https://invest.ppdai.com/api/invapi/ListingListAuthService/listingPagerAuth', data=json.dumps(data), header=header).content.decode())
            res = json.loads(requests.post('https://invest.ppdai.com/api/invapi/ListingListNoAuthService/listingPagerNoAuth', data=json.dumps(data), headers=self.header2, proxies=proxies).content.decode())
            if res['result'] != 1:
                print(res)
                exit(res['result'])
            if not res['resultContent']['dataList']:
                if data['riskLevelCategory'] == 2:
                    return
                data['riskLevelCategory'] += 1
                data['pageIndex'] = 1
                continue
            for each in res['resultContent']['dataList']:
                listing_id = each['listingId']
                print(listing_id)
                if r.sismember('done', listing_id):
                    continue
                else:
                    r.sadd('yet', listing_id)
            data['pageIndex'] += 1
            time.sleep(3)

    def step1(self, listing_id):
        if self.dbWorker.search(Loan.loan_id == listing_id):
            return False
        while True:
            url = 'https://invest.ppdai.com/api/invapi/LoanDetailPcService/showListingBaseInfo'
            data = {
                'listingId': str(listing_id),
                'source': 1,
            }
            res = json.loads(self.post_html(url, header=self.header2, data=json.dumps(data)).content.decode())
            if res['result'] != 1:
                if res['result'] == 404:
                    print(res)
                    return False
                if res['result'] == 5066:
                    self.login()
                    continue
                print(res)
                exit(-1)
            return self.step1_save(res['resultContent'])

    def step1_save(self, result):
        loan_id = result['listing']['listingId']
        title = result['listing']['title']
        stamps = []
        if result['listing']['ispay']:
            stamps.append('赔')
        if result['listing']['vouch'] == 168:
            stamps.append('安')
        if result['listing']['listType'] == 15:
            stamps.append('助农')
        if result['listing']['listType'] == 16:
            stamps.append('富')
        if result['listing']['listType'] == 18:
            stamps.append('车')
        if result['listing']['listType'] == 25:
            stamps.append('助学')
        if result['listing']['isSevenDaysRecede']:
            stamps.append('7天')
        credit = result['listing']['creditCode']
        user_name = result['userInfo']['userName']
        amount = result['listing']['amount']
        current_rate = result['listing']['currentRate']
        months = result['listing']['months']
        if result['repaymentMethod'] == 1:
            repayment_method = '一次还本付息'
        elif result['listing']['listType'] == 16:
            repayment_method = '一次还本付息'
        elif result['listing']['listType'] == 15:
            repayment_method = '月还息，季还1/4本金'
        elif result['listing']['listType'] == 88:
            repayment_method = ''
        else:
            repayment_method = '等额本息（按月还款）'
        try:
            progress = int((result['listing']['funding'] / result['listing']['amount']) * 100)
        except:
            progress = 0
        bid_users = result['totalBidUsers']
        list_end_date = time.strptime(result['listing']['listEndDate'], '%Y-%m-%d %H:%M:%S')
        try:
            bid_status = {
                0: "正在预审中",
                2: "借款审批中",
                4: "借款成功",
                12: "借款成功",
                20: "资金募集中"
            }[result['listing']['statusId']]
        except:
            if result['listing']['statusId'] != 1 or result['listingSaveBid']:
                bid_status = '投标已结束'
            else:
                bid_status = '投标中'
        loan_use = result['loanUse']
        user_id = result['userInfo']['id']
        self.dbWorker.insert(Loan(
            loan_id=loan_id,
            title=title,
            stamps='，'.join(stamps),
            credit=credit,
            user_name=user_name,
            amount=amount,
            current_rate=current_rate,
            months=months,
            repayment_method=repayment_method,
            progress=progress,
            bid_users=bid_users,
            list_end_date=list_end_date,
            bid_status=bid_status,
            loan_use=loan_use,
            user_id=user_id,
        ))
        return user_id

    def step2(self, user_id, listing_id):
        while True:
            url = 'https://invest.ppdai.com/api/invapi/LoanDetailPcService/showBorrowerInfo'
            data = {
                'listingId': str(listing_id),
                'source': 1,
            }
            res = json.loads(self.post_html(url=url, data=json.dumps(data), header=self.header2).content.decode())
            if res['result'] != 1:
                if res['result'] == 5066:
                    self.login()
                    continue
                print(res)
                exit(-2)
            self.step2_save(user_id, res['resultContent'])
            return

    def step2_save(self, user_id, result):
        if self.dbWorker.search(User.user_id == user_id):
            return
        real_name = result['realName']
        id_number = result['idNumber']
        gender = result['gender']
        age = result['age']
        register_date = time.strptime(result['registerDateStr'], '%Y-%m-%d')
        degree = '暂未提供'
        graduation = '暂未提供'
        study_style = '暂未提供'
        if result['educationInfo']:
            degree = result['educationInfo']['educationDegree']
            graduation = result['educationInfo']['graduate']
            study_style = result['educationInfo']['studyStyle']
        if result['overdueStatus'] == 1:
            overdue_status = '有逾期'
        elif result['overdueStatus'] == 2:
            overdue_status = '无逾期'
        else:
            overdue_status = '暂未提供'
        overdue_types = None
        if result['overdueStatus'] == 1 and result['overdueTyps']:
            overdue_types = '，'.join(result['overdueTyps'])
        repayment_source = result['repaymentSourceType']
        work_info = result['workInfo']
        if result['income']:
            income = result['income']
        else:
            income = '暂未提供'
        if result['balAmount']:
            bal_amount = result['balAmount']
        else:
            bal_amount = '暂未提供'
        if result['industry']:
            industry = result['industry']
        else:
            industry = '暂未提供'
        auths = ''
        if result['userAuthsList']:
            auths = '，'.join([each['name'] for each in result['userAuthsList']])
        self.dbWorker.insert(User(
            user_id=user_id,
            real_name=real_name,
            id_number=id_number,
            gender=gender,
            age=age,
            register_date=register_date,
            degree=degree,
            graduation=graduation,
            study_style=study_style,
            overdue_status=overdue_status,
            overdue_types=overdue_types,
            repayment_source=repayment_source,
            work_info=work_info,
            income=income,
            bal_amount=bal_amount,
            industry=industry,
            auths=auths,
        ))

    def step3(self, user_id, listing_id):
        while True:
            url = 'https://invest.ppdai.com/api/invapi/LoanDetailPcService/showBorrowerStatistics'
            data = {
                'listingId': str(listing_id),
                'source': 1,
            }
            res = json.loads(self.post_html(url=url, data=json.dumps(data), header=self.header2).content.decode())
            if res['result'] != 1:
                if res['result'] == 5066:
                    self.login()
                    continue
                print(res)
                exit(-3)
            self.step3_save(user_id, listing_id, res['resultContent'])
            return

    def step3_save(self, user_id, loan_id, result):
        if self.dbWorker.search(Statistic.loan_id == loan_id):
            return
        if result['loanerStatistics']:
            success_borrow_num = result['loanerStatistics']['listingStatics']['successNum']
            try:
                first_success_borrow_date = time.strptime(result['loanerStatistics']['listingStatics']['firstSuccessDate'], '%Y-%m-%d %H:%M:%S')
            except:
                first_success_borrow_date = None
            history = str(result['loanerStatistics']['listingStatics']['wasteNum']) + "次流标，" + str(result['loanerStatistics']['listingStatics']['cancelNum']) + "次撤标，" + str(result['loanerStatistics']['listingStatics']['failNum']) + "次失败"
            success_pay_num = result['loanerStatistics']['successNum']
            normal_num = result['loanerStatistics']['normalNum']
            overdue_less_num = result['loanerStatistics']['overdueLessNum']
            overdue_more_num = result['loanerStatistics']['overdueMoreNum']
            owing_amount_map = result['loanerStatistics']['owingAmountMap']
            overdue_day_map = result['loanerStatistics']['overdueDayMap']
            total_principal = result['loanerStatistics']['totalPrincipal']
            owing_amount = result['loanerStatistics']['owingAmount']
            loan_amount_max = result['loanerStatistics']['loanAmountMax']
            debt_amount_max = result['loanerStatistics']['debtAmountMax']
            debt_amount_map = result['loanerStatistics']['debtAmountMap']
        else:
            success_borrow_num = None
            first_success_borrow_date = None
            history = None
            success_pay_num = None
            normal_num = None
            overdue_less_num = None
            overdue_more_num = None
            owing_amount_map = None
            overdue_day_map = None
            total_principal = None
            owing_amount = None
            loan_amount_max = None
            debt_amount_max = None
            debt_amount_map = None
        if result['otherProjectInfo']:
            business_and_financial_info = result['otherProjectInfo']['businessAndFinancialInfo']
            repayment_power_change = result['otherProjectInfo']['repaymentPowerChange']
            bad_info = result['otherProjectInfo']['badInfo']
            administrative_penalty = result['otherProjectInfo']['administrativePenalty']
        else:
            business_and_financial_info = None
            repayment_power_change = None
            bad_info = None
            administrative_penalty = None
        self.dbWorker.insert(Statistic(
            loan_id=loan_id,
            success_borrow_num=success_borrow_num,
            first_success_borrow_date=first_success_borrow_date,
            history=history,
            success_pay_num=success_pay_num,
            normal_num=normal_num,
            overdue_less_num=overdue_less_num,
            overdue_more_num=overdue_more_num,
            owing_amount_map=owing_amount_map,
            overdue_day_map=overdue_day_map,
            total_principal=total_principal,
            owing_amount=owing_amount,
            loan_amount_max=loan_amount_max,
            debt_amount_max=debt_amount_max,
            debt_amount_map=debt_amount_map,
            business_and_financial_info=business_and_financial_info,
            repayment_power_change=repayment_power_change,
            bad_info=bad_info,
            administrative_penalty=administrative_penalty,
        ))
        if result['loanerStatistics'] and result['loanerStatistics']['previousListings']:
            previous_listing = []
            for each in result['loanerStatistics']['previousListings']:
                try:
                    status = {
                        0: "待审中",
                        1: "正在进行中",
                        2: "待批准",
                        3: "流标",
                        4: "成功",
                        5: "批准失败",
                        6: "借出者付款",
                        7: "借入者收到借款",
                        8: "借入者还款",
                        9: "借出者收到还款",
                        10: "已撤回",
                        11: "草稿",
                        12: "已还完"
                    }[each['statusId']]
                except:
                    status = ''
                previous_listing.append(PreviousListing(
                    user_id=user_id,
                    title=each['title'],
                    rate=each['showRate'],
                    months=each['months'],
                    amount=each['amount'],
                    creation_date=time.strptime(each['creationDate'], '%Y-%m-%d %H:%M:%S'),
                    status=status,
                    loan_id=loan_id,
                ))
            self.dbWorker.insert_all(previous_listing)

    def step4(self, listing_id):
        while True:
            url = 'https://invest.ppdai.com/api/invapi/LoanDetailPcService/showBidRecord'
            data = {
                'listingId': str(listing_id),
                'source': 1,
            }
            res = json.loads(self.post_html(url=url, data=json.dumps(data), header=self.header2).content.decode())
            if res['result'] != 1:
                if res['result'] == 5066:
                    self.login()
                    continue
                print(res)
                exit(-4)
            self.step4_save(listing_id, res['resultContent'])
            return

    def step4_save(self, loan_id, result):
        if not result['bidRecordList']:
            return
        records = []
        for each in result['bidRecordList']:
            try:
                source = {
                    3: "APP投标",
                    4: "自动投标",
                    5: "快投",
                    7: "OpenAPi投标",
                    8: "项目",
                    9: "自动投标",
                    10: "一键投标",
                    12: "一键投标",
                    13: "自动投标",
                    14: "项目",
                    16: "策略",
                    18: "项目",
                    19: "极速投标"
                }[each['source']]
            except:
                source = ''
            records.append(Record(
                loan_id=loan_id,
                lender_id=each['lenderId'],
                lender_name=each['lenderName'],
                source=source,
                rate=each['bidRate'],
                participation_amount=each['participationAmount'],
                creation_date=time.strptime(each['creationDate'], '%Y-%m-%d %H:%M:%S'),
            ))
        self.dbWorker.insert_all(records)

    def step5(self, listing_id):
        while True:
            url = 'https://invest.ppdai.com/api/invapi/LoanDetailPcService/showDebtRecord'
            data = {
                'listingId': str(listing_id),
                'source': 1,
            }
            res = json.loads(self.post_html(url=url, data=json.dumps(data), header=self.header2).content.decode())
            if res['result'] != 1:
                if res['result'] == 5066:
                    self.login()
                    continue
                print(res)
                exit(-5)
            self.step5_save(res['resultContent'])
            return

    def step5_save(self, result):
        if not result['debtRecordList']:
            return
        debt_record = []
        for each in result['debtRecordList']:
            try:
                buy_source_type = {
                    1: "PC投标",
                    2: "APP投标",
                    3: "自动投标",
                    4: "OpenAPi投标",
                    9: "PC一键投标",
                    10: "APP一键投标",
                }[each['buySourceType']]
            except:
                buy_source_type = '项目'
            debt_record.append(DebtRecord(
                lender_id=each['lenderId'],
                lender_name=each['lenderName'],
                owing_principal=each['owingPrincipal'],
                price_for_sell=each['priceForSell'],
                buy_source_type=buy_source_type,
                close_bid_date=time.strptime(each['closeBidDate'], '%Y-%m-%d %H:%M:%S'),
                debt_deal_id=str(each['debtDealId']),
                buyer_user_name=each['buyerUserName'],
            ))
        self.dbWorker.insert_all(debt_record)

    def run(self):
        while True:
            ld = r.spop('yet')
            if not ld:
                print('sleep for 2h')
                time.sleep(2*60*60)
                continue
            ld = int(ld.decode())
            print(ld)
            r.sadd('done', ld)
            user_id = self.step1(ld)
            if not user_id:
                continue
            time.sleep(1)
            self.step2(user_id, ld)
            time.sleep(1)
            self.step3(user_id, ld)
            time.sleep(1)
            self.step4(ld)
            time.sleep(1)
            self.step5(ld)
            time.sleep(1)


if __name__ == '__main__':
    spider = Spider()
    while True:
        spider.step1(10000000)
        print('sleep for 2h')
        time.sleep(2*60*60)
    # spider.run()
