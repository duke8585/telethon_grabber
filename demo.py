import json
import pandas as pd

# TODO
#
#
# resources
# https://tl.telethon.dev/methods/index.html
# https://medium.com/better-programming/how-to-get-data-from-telegram-82af55268a4b


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

def extract_from_json(message):
    # print(message)
    return (message['from_id']['user_id'], message['date'], message['message'])

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
    import telethon

    offset_id = 0
    limit = 1000 # messages per run
    all_messages = []
    total_messages = 0
    total_count_limit = 5000 # total limit

    rows = []

    while True:
        print("Current Offset ID is:", offset_id, "; Total Messages:", total_messages)
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
            #print(message, type(message))
            if isinstance(message, telethon.types.Message):
                # handling in case of MessageService occurences (rare)
                all_messages.append(message.to_dict())
                data_row = extract_from_json(message.to_dict())
                rows.append(data_row)
        offset_id = messages[len(messages) - 1].id
        total_messages = len(all_messages)
        if total_count_limit != 0 and total_messages >= total_count_limit:
            break

    return rows

def has_attr(object, attr):
    if has_attr(object, attr):
        return True
    else:
        return False

if __name__ == '__main__':
    get_config()
    client = create_client()
    my_chats = get_chats(client)

    #print(list(dir(chats[0]))) # chat object dir

    allowed = ['CryptoMoon', 'Facemelters Spotlight', '🍤 Shrimp Tank 🍤', 'BabyWhaleX']
    chats = [chat for chat in my_chats if chat.title in allowed]
    print('chat.titles: ', [chat.title for chat in chats])

    #channel = get_channel(client) # for public ones

    for chat in chats:
        print('>>> starting: ', chat.title)

        rows = get_channel_messages(client, chat)
        #print('\n'.join([str(r) for r in rows]))

        df = pd.DataFrame(rows, columns=['user_id', 'date', 'message'])

        title = chat.title.replace('🍤', '').strip().replace(' ', '_')
        print(title)

        df.loc[:, 'source'] = title
        print(df.shape, df.head(5))

        df.to_pickle(f'df.pickle')

    # get channel ids
    #print([(chat.title, chat.id, lambda c: c.date() if has_attr(c, 'date') else 0) for chat in chats])

    # get participants
    #all_participants = client.get_participants(chats[0], aggressive=True)
    #print(all_participants)




