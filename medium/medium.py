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
market_data['timestamp'] = market_data['timestamp'].astype('int32')
market_data['bar'] = market_data['bar'].astype('int32')
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

###################

merged_data.drop(['currency', 'bar_x', 'bar_y'], axis=1, inplace=True)
merged_data.loc[:, 'delta'] = merged_data['delta'] * merged_data['price']
merged_data.drop(['price'], axis=1, inplace=True)


### Get all timestamps current balances should be calculated for
bar_set = set(range(user_data['bar'].min(), max(market_data['bar'].max(), user_data['bar'].max()) + COEF, COEF))
timestamp_set = set(market_data['timestamp'][market_data['timestamp'] >= user_data['timestamp'].min()]).union(set(user_data['timestamp']))
timestamp_bar_set = set(market_data['bar'][market_data['bar'] >= user_data['bar'].min()]).union(set(user_data['bar']))
final_set = (bar_set - timestamp_bar_set).union(timestamp_set)
final_set = sorted(final_set)


### Create a Dataframe for keeping current balances
user_balances = pd.DataFrame(index=final_set,
                             columns=user_data['user_id'].unique(),
                             data=0.0,
                             dtype=np.float32)
for row in merged_data.itertuples():
    user_balances.at[row.timestamp, row.user_id] = row.delta
user_balances = user_balances.cumsum()
# Convert the index to a column for merging
user_balances.reset_index(inplace=True)
user_balances.rename(columns={'index': 'timestamp'}, inplace=True)
user_balances['timestamp'] = user_balances['timestamp'].astype('int32')


### Duplicate rows to add information about current balance after previous time bar
COEF = 3600
user_balances['bar'] = (user_balances['timestamp'] // COEF) * COEF
max_timestamps = user_balances.groupby('bar')['timestamp'].max().values[:-1]
df_duplicate = user_balances[user_balances['timestamp'].isin(max_timestamps)]
df_duplicate.loc[:, 'bar'] = df_duplicate['bar'] + COEF
df_duplicate.loc[:, 'timestamp'] = df_duplicate['bar']
user_balances = pd.concat([user_balances, df_duplicate])
df_duplicate = user_balances[user_balances['bar'] == user_balances['bar'].min()]
df_duplicate.loc[:, 'timestamp'] = df_duplicate['bar']
df_duplicate.loc[:, user_data['user_id'].unique().tolist()] = 0.0
user_balances = pd.concat([user_balances, df_duplicate]).sort_values(by='timestamp')


### Calculate aggregated statistics
users = user_data['user_id'].unique()
groups_1h = user_balances.groupby(['bar'])
aggregated_1h = groups_1h[users[0]].aggregate(minimum_balance='min',
                               maximum_balance='max',
                               average_balance='mean')
aggregated_1h['user_id'] = users[0]
for user in users[1:]:
    temp = groups_1h[user].aggregate(minimum_balance='min',
                               maximum_balance='max',
                               average_balance='mean')
    temp['user_id'] = user
    aggregated_1h = pd.concat([aggregated_1h, temp])
aggregated_1h.reset_index(inplace=True)
aggregated_1h.rename(columns={'bar': 'start_timestamp'}, inplace=True)
cols = ['user_id', 'minimum_balance', 'maximum_balance', 'average_balance', 'start_timestamp']
aggregated_1h = aggregated_1h[cols]
aggregated_1h.to_csv('bars-1h.csv', index=False, float_format='%.4f')


### Do aggregation for a day frequency
COEF = 3600 * 24
market_data['bar'] = (market_data['timestamp'] // COEF) * COEF
market_data['bar'] = market_data['bar'].astype('int32')
user_data['bar'] = (user_data['timestamp'] // COEF) * COEF
user_data['bar'] = user_data['bar'].astype('int32')
bar_set = set(range(user_data['bar'].min(), max(market_data['bar'].max(), user_data['bar'].max()) + COEF, COEF))
timestamp_set = set(market_data['timestamp'][market_data['timestamp'] >= user_data['timestamp'].min()]).union(set(user_data['timestamp']))
timestamp_bar_set = set(market_data['bar'][market_data['bar'] >= user_data['bar'].min()]).union(set(user_data['bar']))
final_set = (bar_set - timestamp_bar_set).union(timestamp_set)
final_set = sorted(final_set)

######################################

balances = user_balances[user_balances['timestamp'].isin(final_set)]
COEF = 3600 * 24
balances.loc[:, 'bar'] = (balances['timestamp'] // COEF) * COEF
max_timestamps = balances.groupby('bar')['timestamp'].max().values[:-1]
df_duplicate = balances[balances['timestamp'].isin(max_timestamps)]
df_duplicate.loc[:, 'bar'] = df_duplicate['bar'] + COEF
df_duplicate.loc[:, 'timestamp'] = df_duplicate['bar']
balances = pd.concat([balances, df_duplicate])
df_duplicate = balances[balances['bar'] == balances['bar'].min()]
df_duplicate.loc[:, 'timestamp'] = df_duplicate['bar']
df_duplicate.loc[:, user_data['user_id'].unique().tolist()] = 0.0
balances = pd.concat([balances, df_duplicate]).sort_values(by='timestamp')
groups_1d = balances.groupby(['bar'])
aggregated_1d = groups_1d[users[0]].aggregate(minimum_balance='min',
                               maximum_balance='max',
                               average_balance='mean')
aggregated_1d['user_id'] = users[0]
for user in users[1:]:
    temp = groups_1d[user].aggregate(minimum_balance='min',
                               maximum_balance='max',
                               average_balance='mean')
    temp['user_id'] = user
    aggregated_1d = pd.concat([aggregated_1d, temp])
aggregated_1d.reset_index(inplace=True)
aggregated_1d.rename(columns={'bar': 'start_timestamp'}, inplace=True)
cols = ['user_id', 'minimum_balance', 'maximum_balance', 'average_balance', 'start_timestamp']
aggregated_1d = aggregated_1d[cols]
aggregated_1d.to_csv('bars-1d.csv', index=False, float_format='%.4f')


### Do aggregation for a month frequency
COEF = 3600 * 24 * 30
market_data.loc[:, 'bar'] = (market_data['timestamp'] // COEF) * COEF
market_data['bar'] = market_data['bar'].astype('int32')
user_data.loc[:, 'bar'] = (user_data['timestamp'] // COEF) * COEF
user_data['bar'] = user_data['bar'].astype('int32')
bar_set = set(range(user_data['bar'].min(), max(market_data['bar'].max(), user_data['bar'].max()) + COEF, COEF))
timestamp_set = set(market_data['timestamp'][market_data['timestamp'] >= user_data['timestamp'].min()]).union(set(user_data['timestamp']))
timestamp_bar_set = set(market_data['bar'][market_data['bar'] >= user_data['bar'].min()]).union(set(user_data['bar']))
final_set = (bar_set - timestamp_bar_set).union(timestamp_set)
final_set = sorted(final_set)
balances_month = balances[balances['timestamp'].isin(final_set)]
COEF = 3600 * 24 * 30
balances_month.loc[:, 'bar'] = (balances_month['timestamp'] // COEF) * COEF
max_timestamps = balances_month.groupby('bar')['timestamp'].max().values[:-1]
df_duplicate = balances_month[balances_month['timestamp'].isin(max_timestamps)]
df_duplicate.loc[:, 'bar'] = df_duplicate['bar'] + COEF
df_duplicate.loc[:, 'timestamp'] = df_duplicate['bar']
balances_month = pd.concat([balances_month, df_duplicate])
df_duplicate = balances_month[balances_month['bar'] == balances_month['bar'].min()]
df_duplicate.loc[:, 'timestamp'] = df_duplicate['bar']
df_duplicate.loc[:, user_data['user_id'].unique().tolist()] = 0.0
balances_month = pd.concat([balances_month, df_duplicate]).sort_values(by='timestamp')
groups_1m = balances_month.groupby(['bar'])
aggregated_1m = groups_1m[users[0]].aggregate(minimum_balance='min',
                               maximum_balance='max',
                               average_balance='mean')
aggregated_1m['user_id'] = users[0]
for user in users[1:]:
    temp = groups_1m[user].aggregate(minimum_balance='min',
                               maximum_balance='max',
                               average_balance='mean')
    temp['user_id'] = user
    aggregated_1m = pd.concat([aggregated_1m, temp])
aggregated_1m.reset_index(inplace=True)
aggregated_1m.rename(columns={'bar': 'start_timestamp'}, inplace=True)
cols = ['user_id', 'minimum_balance', 'maximum_balance', 'average_balance', 'start_timestamp']
aggregated_1m = aggregated_1m[cols]
aggregated_1m.to_csv('bars-30d.csv', index=False, float_format='%.4f')