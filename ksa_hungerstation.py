import logging
import requests
import pandas as pd
from logger import custom_logger
logger = custom_logger(__name__)
BRANCH_IDS = []
TOKEN = None


def create_session(email: str, password: str, headers: dict) -> requests.Session:
    global TOKEN
    url = 'https://bff-api.eu.prd.portal.restaurant/auth/v4/oneweb/login'
    json = {
        'username': email,
        'password': password
    }
    session = requests.Session()
    response = session.post(url, json=json, headers=headers)
    if response.status_code == 200:
        TOKEN = response.json()['keymaker']['access_token']
        branch_ids_data = response.json()['profile']['accounts']
        for i in branch_ids_data:
            BRANCH_IDS.append(i['vendor_id'])
        return session
    else:
        logging.error(f'Error in returning session: {response.text}')
        raise Exception('Error in returning session')


def get_campaign_ids(session: requests.Session, branch_id: str, headers: dict, token: str) -> list[dict]:
    url = f'https://at-vc-gtw.deliveryhero.io/api/v1/entities/HS_SA/vendors/{branch_id}/campaigns'
    headers.update({
        'x-vendorid': branch_id,
        'authorization': f'Bearer {token}',
    })
    params = {
        'ended_after': '2024-01-01',
        'product': 'premium_placements',
        'pricing_model': 'CPC'
    }
    response = session.get(url, headers=headers, params=params)
    if response.status_code == 200:
        response_data = response.json()['data']
        if response_data:
            return response_data
        else:
            return []
    else:
        logging.error(f'Error in returning session: {response.text}')


def get_campaign_data(session: requests.Session, branch_id: str, headers: dict, token: str, campaign_id: str) -> list[
    dict]:
    url = 'https://at-vc-gtw.deliveryhero.io/api/v1/entities/HS_SA/reporting'
    headers.update({
        'x-vendorid': str(branch_id),
        'authorization': f'Bearer {token}'
    })
    params = {
        'product_type': 'premium_placements',
        'campaign_ids': campaign_id,
        'aggregation_level': 'daily',
        'filter_zero_clicks': 'false',
        'include_segment': 'true',
    }
    response = session.get(url, headers=headers, params=params)
    if response.status_code == 200:
        response_data = response.json()['data']['campaigns']
        if response_data:
            return response_data
        else:
            return []
    else:
        logging.error(f'Error in returning session: {response.text}')


def parse_campaigns_data(campaign_data: dict) -> dict:
    return {
        'campaign_date': campaign_data['startDate'],
        'orders': campaign_data['orders'],
        'revenue': campaign_data['revenue'],
        'roi': campaign_data['returnOnAdSpend'],
        'conversion': campaign_data['clicksToOrders'],
        'aov': campaign_data['averageOrderValue'],
        'clicks': campaign_data['clicks'],
        'bid': campaign_data['bid']}


def ksa_main():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9',
        'content-type': 'application/json',
        'origin': 'https://partner-app.hungerstation.com',
        'priority': 'u=1, i',
        'referer': 'https://partner-app.hungerstation.com/',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
        'x-app-name': 'one-web',
        'x-app-version': '2.6.2',
    }
    campaigns_ids: list[tuple] = []
    campaigns_data: list[dict] = []
    output = []
    with create_session('adeeb@kitopi.com', 'Aadeeb123456@', headers) as s:
        for branch_id in BRANCH_IDS:
            campaign_ids = get_campaign_ids(session=s, branch_id=branch_id, headers=headers, token=TOKEN)
            if campaign_ids:
                for campaign_id in campaign_ids:
                    campaigns_ids.append((branch_id, campaign_id['id']))

        for index, campaign in enumerate(campaigns_ids):
            branch_id = str(campaigns_ids[index][0])
            campaign_id = campaigns_ids[index][1]
            campaign_details = get_campaign_data(session=s, branch_id=branch_id, headers=headers, token=TOKEN,campaign_id=campaign_id)
            if campaign_details:
                campaigns_data.append(campaign_details[0])


    for campaign_data in campaigns_data:
        branch_id = campaign_data['vendorId']
        daily_data = campaign_data['cpcMetrics']
        for day in daily_data:
            parse_data = parse_campaigns_data(day)
            parse_data['branch_id'] = branch_id
            output.append(parse_data)


    df = pd.DataFrame(output)
    df.to_csv('./data/ksa_cpc.csv', index=False)


if __name__ == '__main__':
    ksa_main()
