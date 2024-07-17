import pandas as pd
import numpy as np


COEF = 3600

path_market = 'market_data.csv'
path_user = 'user_data.csv'
market_data = pd.read_csv(path_market)
market_data['bar'] = (market_data['timestamp'] // COEF) * COEF
market_data['currency'] = market_data['symbol'].apply(lambda x: x[:3])
market_data.drop(['symbol'], axis=1, inplace=True)

################

market_data.loc[-1] = [0, 1.0, 0, 'USD']
market_data.index = market_data.index + 1
market_data.sort_index(inplace=True)

##################

market_data['currency'] = market_data['currency'].astype('category')
market_data['price'] = market_data['price'].astype('float32')
market_data[['timestamp', 'bar']] = market_data[['timestamp', 'bar']].astype('int32')
user_data = pd.read_csv(path_user)
user_data['bar'] = (user_data['timestamp'] // COEF) * COEF

#######################

user_data[['currency', 'user_id']] = user_data[['currency', 'user_id']].astype('category')
user_data['delta'] = user_data['delta'].astype('float32')
user_data[['timestamp', 'bar']] = user_data[['timestamp', 'bar']].astype('int32')
user_data['bar'] = user_data['bar'].astype('int32')


### Get Dataframe with balances already converted to USD
user_data.sort_values(by=['timestamp', 'currency'], inplace=True)
market_data.sort_values(by=['timestamp', 'currency'], inplace=True)
merged_data = pd.merge_asof(user_data, market_data, on='timestamp', by='currency', direction='backward')
merged_data.drop(['currency', 'bar_x', 'bar_y'], axis=1, inplace=True)
merged_data.loc[:, 'delta'] = merged_data['delta'] * merged_data['price']
merged_data.drop(['price'], axis=1, inplace=True)


### Get all timestamps current balances should be calculated for
bar_set = set(range(user_data['bar'].min(), max(market_data['bar'].max(), user_data['bar'].max()) + COEF, COEF))
timestamp_set = set(market_data['timestamp'][market_data['timestamp'] >= user_data['timestamp'].min()]).union(set(user_data['timestamp']))
timestamp_bar_set = set(market_data['bar'][market_data['bar'] >= user_data['bar'].min()]).union(set(user_data['bar']))
final_set = (bar_set - timestamp_bar_set).union(timestamp_set)
final_set = [0] + sorted(final_set)


### Create a Dataframe for keeping current balances
user_balances = pd.DataFrame(index=final_set,
                             columns=user_data['user_id'].unique(),
                             data=0.0,
                             dtype=np.float32)
for row in merged_data.itertuples():
    user_balances.at[row.timestamp, row.user_id] = row.delta
for col in user_balances.columns:
    user_balances.loc[:, col] = user_balances[col].cumsum()
# Convert the index to a column for merging
user_balances.reset_index(inplace=True)
user_balances.rename(columns={'index': 'timestamp'}, inplace=True)
user_balances['timestamp'] = user_balances['timestamp'].astype('int32')


### Duplicate rows to add information about current balance after previous time bar
COEF = 3600
user_balances['bar'] = (user_balances['timestamp'] // COEF) * COEF
user_balances.at[0, 'timestamp'] = user_balances.at[1, 'bar'] - COEF
user_balances.at[0, 'bar'] = user_balances.at[1, 'bar'] - COEF
users = user_data['user_id'].unique().tolist()
bars = user_balances['bar'].unique()


### Calculate aggregated statistics
pd_1h = pd.DataFrame()
pd_1h['user_id'] = users * (len(bars) - 1)
pd_1h['user_id'] = pd_1h['user_id'].astype('category')
pd_1h['minimum_balance'] = 0
pd_1h['minimum_balance'] = pd_1h['minimum_balance'].astype('float32')
pd_1h['maximum_balance'] = 0
pd_1h['maximum_balance'] = pd_1h['maximum_balance'].astype('float32')
pd_1h['average_balance'] = 0
pd_1h['average_balance'] = pd_1h['average_balance'].astype('float32')
pd_1h['start_timestamp'] = np.repeat(bars[1:], len(users))
pd_1h['start_timestamp'] = pd_1h['start_timestamp'].astype('int32')
for i, bar in enumerate(bars):
    # If not the first group, add the last value of the previous group
    current_group = user_balances[user_balances['bar'] == bar]
    if i > 0:
        current_group = pd.concat([previous_group_last_value, current_group])
        pd_1h.loc[(i - 1) * len(users) : i * len(users) - 1, 'minimum_balance'] = current_group[users].min(axis=0).values
        pd_1h.loc[(i - 1) * len(users) : i * len(users) - 1, 'maximum_balance'] = current_group[users].max(axis=0).values
        pd_1h.loc[(i - 1) * len(users) : i * len(users) - 1, 'average_balance'] = current_group[users].mean(axis=0).values
    previous_group_last_value = current_group.iloc[[-1]]
pd_1h.to_csv('bars-1h.csv', index=False, float_format='%.4f')


### Do aggregation for a day frequency
COEF = 3600 * 24
user_balances.loc[:, 'bar'] = (user_balances['timestamp'] // COEF) * COEF
user_balances.at[0, 'timestamp'] = user_balances.at[1, 'bar'] - COEF
user_balances.at[0, 'bar'] = user_balances.at[1, 'bar'] - COEF
bars = user_balances['bar'].unique()
pd_1d = pd.DataFrame()
pd_1d['user_id'] = users * (len(bars) - 1)
pd_1d['user_id'] = pd_1d['user_id'].astype('category')
pd_1d['minimum_balance'] = 0
pd_1d['minimum_balance'] = pd_1d['minimum_balance'].astype('float32')
pd_1d['maximum_balance'] = 0
pd_1d['maximum_balance'] = pd_1d['maximum_balance'].astype('float32')
pd_1d['average_balance'] = 0
pd_1d['average_balance'] = pd_1d['average_balance'].astype('float32')
pd_1d['start_timestamp'] = np.repeat(bars[1:], len(users))
pd_1d['start_timestamp'] = pd_1d['start_timestamp'].astype('int32')
for i, bar in enumerate(bars):
    # If not the first group, add the last value of the previous group
    current_group = user_balances[user_balances['bar'] == bar]
    if i > 0:
        current_group = pd.concat([previous_group_last_value, current_group])
        pd_1d.loc[(i - 1) * len(users) : i * len(users) - 1, 'minimum_balance'] = current_group[users].min(axis=0).values
        pd_1d.loc[(i - 1) * len(users) : i * len(users) - 1, 'maximum_balance'] = current_group[users].max(axis=0).values
        pd_1d.loc[(i - 1) * len(users) : i * len(users) - 1, 'average_balance'] = current_group[users].mean(axis=0).values
    previous_group_last_value = current_group.iloc[[-1]]
pd_1d.to_csv('bars-1d.csv', index=False, float_format='%.4f')


### Do aggregation for a month frequency
COEF = 3600 * 24 * 30
user_balances.loc[:, 'bar'] = (user_balances['timestamp'] // COEF) * COEF
user_balances.at[0, 'timestamp'] = user_balances.at[1, 'bar'] - COEF
user_balances.at[0, 'bar'] = user_balances.at[1, 'bar'] - COEF
bars = user_balances['bar'].unique()
pd_30d = pd.DataFrame()
pd_30d['user_id'] = users * (len(bars) - 1)
pd_30d['user_id'] = pd_30d['user_id'].astype('category')
pd_30d['minimum_balance'] = 0
pd_30d['minimum_balance'] = pd_30d['minimum_balance'].astype('float32')
pd_30d['maximum_balance'] = 0
pd_30d['maximum_balance'] = pd_30d['maximum_balance'].astype('float32')
pd_30d['average_balance'] = 0
pd_30d['average_balance'] = pd_30d['average_balance'].astype('float32')
pd_30d['start_timestamp'] = np.repeat(bars[1:], len(users))
pd_30d['start_timestamp'] = pd_30d['start_timestamp'].astype('int32')
for i, bar in enumerate(bars):
    # If not the first group, add the last value of the previous group
    current_group = user_balances[user_balances['bar'] == bar]
    if i > 0:
        current_group = pd.concat([previous_group_last_value, current_group])
        pd_30d.loc[(i - 1) * len(users) : i * len(users) - 1, 'minimum_balance'] = current_group[users].min(axis=0).values
        pd_30d.loc[(i - 1) * len(users) : i * len(users) - 1, 'maximum_balance'] = current_group[users].max(axis=0).values
        pd_30d.loc[(i - 1) * len(users) : i * len(users) - 1, 'average_balance'] = current_group[users].mean(axis=0).values
    previous_group_last_value = current_group.iloc[[-1]]
pd_30d.to_csv('bars-30d.csv', index=False, float_format='%.4f')