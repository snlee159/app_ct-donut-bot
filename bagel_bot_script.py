from slacker import Slacker
import time
from datetime import datetime as dt
import pandas as pd

slack_bagel = Slacker(bagel_token)

# Get channel info
channel_dict = {}
i = 1000
response = slack_bagel.conversations.list(limit=1000, exclude_archived=True)
channels = response.body['channels']
for channel in channels:
    channel_dict[channel['name']] = channel['id']
while len(response.body['response_metadata']['next_cursor']) > 0:
    i += 1000
    response = slack_bagel.conversations.list(limit=1000, exclude_archived=True,
                                              cursor=response.body['response_metadata']['next_cursor'])
    channels = response.body['channels']
    for channel in channels:
        channel_dict[channel['name']] = channel['id']
    time.sleep(5)

# Get user info
ds_donut_id = channel_dict['virtual-coffee']

user_info_list = []
response = slack_bagel.conversations.members(channel=ds_donut_id, limit=1000)
user_list = response.body['members']
for user in user_list:
    response = slack_bagel.users.info(user=user, include_locale=True)
    user_info_list += [response.body['user']]

user_df = pd.DataFrame(user_info_list)[['id', 'name', 'real_name', 'tz']]
user_df = user_df[(~user_df.name.str.contains('donut')) &
                  (~user_df.name.str.contains('bagel'))].reset_index(drop=True)

# Match across timezones and with those they haven't matched with yet
history_df = pd.read_csv('donut_history.csv')
possible_cases_df = pd.DataFrame(
    columns=['name1', 'name2', 'times_paired', 'is_diff_tz'])
ind = 0
for i in range(0, len(user_df['name'].tolist())):
    name1 = user_df['name'][i]
    for j in range(i + 1, len(user_df['name'].tolist())):
        name2 = user_df['name'][j]
        tmp_hist_df = history_df[((history_df['name1'] == name1) &
                                  (history_df['name2'] == name2)) |
                                 ((history_df['name2'] == name1) &
                                  (history_df['name1'] == name2))]
        times_paired = len(tmp_hist_df.index)
        is_diff_tz = user_df[user_df['name'] ==
                             name1]['tz'].values[0] != user_df[user_df['name'] == name2]['tz'].values[0]
        possible_cases_df.loc[ind] = [name1, name2, times_paired, is_diff_tz]
        ind += 1

possible_cases_df['match_strength'] = (
    possible_cases_df['is_diff_tz']) - possible_cases_df['times_paired']*2

filter_cases_df = possible_cases_df.copy(deep=True)
match_df = pd.DataFrame(columns=['name1', 'name2'])
ind = 0
for user in user_df['name'].tolist():
    top_user_match = filter_cases_df[(filter_cases_df['name1'] == user) |
                                     (filter_cases_df['name2'] == user)].sort_values('match_strength',
                                                                                     ascending=False).reset_index(
        drop=True)[['name1', 'name2']].head(1).reset_index(drop=True)
    if len(top_user_match.index) > 0:
        name1 = top_user_match.name1.values[0]
        name2 = top_user_match.name2.values[0]
        match_df.loc[ind] = [name1, name2]
        filter_cases_df = filter_cases_df[(filter_cases_df['name1'] != name1) &
                                          (filter_cases_df['name2'] != name1)]
        filter_cases_df = filter_cases_df[(filter_cases_df['name1'] != name2) &
                                          (filter_cases_df['name2'] != name2)]
        ind += 1

# Find if anyone wasn't matched, make a second match with their top option
for user in user_df['name'].tolist():
    tmp_match_df = match_df[(match_df['name1'] == user) |
                            (match_df['name2'] == user)]
    if len(tmp_match_df.index) == 0:
        print(
            f'User: {user} was not matched. Setting a second match up for them...')
        top_user_match = possible_cases_df[(possible_cases_df['name1'] == user) |
                                           (possible_cases_df['name2'] == user)].sort_values('match_strength',
                                                                                             ascending=False).reset_index(
            drop=True)[['name1', 'name2']].head(1).reset_index(drop=True)
        name1 = top_user_match.name1.values[0]
        name2 = top_user_match.name2.values[0]
        match_df.loc[ind] = [name1, name2]

today = dt.strftime(dt.now(), "%Y-%m-%d")
match_df['match_date'] = today
history_df = pd.concat([history_df, match_df])
history_df.to_csv('donut_history.csv', index=False)

# Set up direct messages and message users
for i in range(0, len(match_df.index)):
    user1 = match_df[match_df.index == i].name1.values[0]
    user2 = match_df[match_df.index == i].name2.values[0]
    user1_id = user_df[user_df['name'] == user1]['id'].values[0]
    user2_id = user_df[user_df['name'] == user2]['id'].values[0]
    response = slack_bagel.conversations.open(
        users=[user1_id, user2_id], return_im=True)
    conv_id = response.body['channel']['id']
    response = slack_bagel.chat.post_message(channel=conv_id,
                                             text=f'Hello <@{user1_id}> and <@{user2_id}>! Welcome to your chat space for this round of Bagel! Please use this chat to set up time to hangout!',
                                             as_user='@bagel-bot')

# Send pairings to the ds_donut channel
response = slack_bagel.chat.post_message(channel=ds_donut_id,
                                         text='The new round of pairings are in! You should have received a DM from bagel-bot with your new partner. Please post a photo here of your chat. Chat, chat away!',
                                         as_user='@bagel_bot')
for i in range(0, len(match_df.index)):
    user1 = match_df[match_df.index == i].name1.values[0]
    user2 = match_df[match_df.index == i].name2.values[0]
    user1_id = user_df[user_df['name'] == user1]['id'].values[0]
    user2_id = user_df[user_df['name'] == user2]['id'].values[0]
    response = slack_bagel.chat.post_message(channel=ds_donut_id,
                                             text=f'<@{user1_id}> and <@{user2_id}>',
                                             as_user='@bagel-bot')
