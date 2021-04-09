from datetime import datetime
import argparse
import pandas as pd
import telethon
#import json
#import sys

parser = parser = argparse.ArgumentParser(description='grab messages, timestamps and user_ids from telegram groups')
parser.add_argument('lower_date', metavar='d', type=str, action='store', default='2021-03-01', help='lower date limit, string, e.g. 2021-03-01')
args = parser.parse_args()
DATE_LIMIT_LOW = datetime.strptime(vars(args).get('lower_date'), '%Y-%m-%d')

def get_config():
    """read config from file, see https://my.telegram.org/apps"""
    import configparser
    import telethon.sync # needed, see https://github.com/amiryousefi/telegram-analysis/issues/1

    # make them global
    global username, phone, api_id, api_hash

    # Reading Configs
    config = configparser.ConfigParser()
    config.read("config.ini")

    # Setting configuration values
    api_id = config['Telegram']['api_id']
    api_hash = config['Telegram']['api_hash']

    api_hash = str(api_hash)

    phone = config['Telegram']['phone']
    username = config['Telegram']['username']


def create_client():
    """create client object, with phone code if needed"""
    from telethon import TelegramClient
    from telethon.errors import SessionPasswordNeededError

    client = TelegramClient(username, api_id, api_hash)
    client.start()
    print("Client Created")

    # Ensure you're authorized
    if not client.is_user_authorized():
        client.send_code_request(phone)
        try:
            client.sign_in(phone, input('Enter the code: '))
        except SessionPasswordNeededError:
            client.sign_in(password=input('Password: '))
    return client

def get_channel(client):
    """for rather public ones, get_groups used for private ones"""
    from telethon.tl.types import (
        PeerChannel
    )

    input_manual = False

    if input_manual:
        user_input_channel = input("enter entity(telegram URL or entity id):")

        if user_input_channel.isdigit():
            entity = PeerChannel(int(user_input_channel))
        else:
            entity = user_input_channel

    entity = 'https://t.me/BlockstackChat'

    #entity = 1412607603

    channel = client.get_entity(entity)

    return channel

def get_channel_users(client, channel):
    from telethon.tl.functions.channels import GetParticipantsRequest
    from telethon.tl.types import ChannelParticipantsSearch

    offset = 0
    limit = 100
    all_participants = []

    while True:
        participants = client(GetParticipantsRequest(
            channel, ChannelParticipantsSearch(''), offset, limit,
            hash=0
        ))
        if not participants.users:
            break
        all_participants.extend(participants.users)
        offset += len(participants.users)

    print(all_participants)

def process_messages(messages):
    all_messages = []
    data_rows = []
    for message in messages:
        # print(message, type(message))
        if isinstance(message, telethon.types.Message):
            # handling in case of MessageService occurrences (rare)
            all_messages.append(message.to_dict())
            data_row = extract_from_json(message.to_dict())
            data_rows.append(data_row)
            record_timestamp = data_row[1]  # date is element 2
            print(record_timestamp, limit_reached(record_timestamp))
            if limit_reached(record_timestamp):
                break
    return len(all_messages), data_rows

def extract_from_json(message):
    """return tuple of user_id, date and message"""
    return (message['from_id']['user_id'], message['date'], message['message'])

def limit_reached(date_string):
    """compare date of record to breakup constant, TRUE when lower than LOW_LIMIT"""
    # record_dt = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S+00:00')
    return date_string.replace(tzinfo=None) < DATE_LIMIT_LOW

def get_chats(client):
    """get groups from my account"""
    # from https://python.gotrained.com/scraping-telegram-group-members-python-telethon/
    from telethon.tl.functions.messages import GetDialogsRequest
    from telethon.tl.types import InputPeerEmpty
    chats = []
    last_date = None
    chunk_size = 200
    #groups = []

    result = client(GetDialogsRequest(
        offset_date=last_date,
        offset_id=0,
        offset_peer=InputPeerEmpty(),
        limit=chunk_size,
        hash=0
    ))
    chats.extend(result.chats)

    return chats

def get_channel_messages(client, channel):
    from telethon.tl.functions.messages import (GetHistoryRequest)

    offset_id = 0
    limit = 2000 # messages per run
    all_messages = []
    total_messages = 0
    total_count_limit = 10000 # total limit

    data_rows = []

    while True:
        print("--- Current Offset ID is:", offset_id, "; Total Messages:", total_messages)
        history = client(GetHistoryRequest(
            peer=channel,
            offset_id=offset_id,
            offset_date=None,
            add_offset=0,
            limit=limit,
            max_id=0,
            min_id=0,
            hash=0
        ))
        if not history.messages:
            break
        messages = history.messages

        for message in messages:
            # print(message, type(message))
            if isinstance(message, telethon.types.Message):
                # handling in case of MessageService occurrences (rare)
                all_messages.append(message.to_dict())
                data_row = extract_from_json(message.to_dict())
                record_timestamp = data_row[1]
                data_rows.append(data_row)
        offset_id = messages[len(messages) - 1].id

        ## either terminate when lower date limit is reached or when total messages limit exceeded
        record_timestamp = data_row[1]
        # this is the last record processed in this batch,
        # i.e. lowest date, date value is element 2
        if limit_reached(record_timestamp):
            print(f'>>> stopping at {record_timestamp}')
            break
        total_messages = len(all_messages)
        if total_count_limit != 0 and total_messages >= total_count_limit:
            print(f'>>> stopping after {total_messages} records')
            break

    return data_rows

if __name__ == '__main__':
    get_config()
    client = create_client()
    my_chats = get_chats(client)

    allowed = ['CryptoMoon', 'Facemelters Spotlight', '🍤 Shrimp Tank 🍤', 'BabyWhaleX']
    chats = [chat for chat in my_chats if chat.title in allowed and hasattr(chat, 'megagroup')]
    # megagroup for skipping announcement channels with the same name
    print('chat.titles: ', [chat.title for chat in chats])

    df_list = []
    for chat in chats:
        print('>>> starting: ', chat.title) #, type(chat), dir(chat), chat)

        rows = get_channel_messages(client, chat)
        df = pd.DataFrame(rows, columns=['user_id', 'date', 'message'])
        title = chat.title.replace('🍤', '').strip().replace(' ', '_') # remove special characters
        df.loc[:, 'source'] = title
        print(f'''>>>  df rows: {df.shape[0]}, min date: {df.date.min().date()}, max date: {df.date.max().date()}, df sample:\n''',
              df.sample(2))
        print('>>> finished: ', chat.title)
        df_list.append(df)

    main_df = pd.concat(df_list)
    main_df.to_pickle(f'df.pickle')