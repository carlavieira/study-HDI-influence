import os
import json
from time import sleep

import requests
import pandas as pd
import random
from dotenv import load_dotenv
from pathlib import Path

#
# from resources.utils import get_project_root
load_dotenv()


def get_project_root() -> Path:
	return Path(__file__).parent.parent


URL = 'https://api.github.com/graphql'
TOKEN_LIST = json.loads(os.getenv("GITHUB_ACCESS_TOKENS"))
random.shuffle(TOKEN_LIST)
ROOT = get_project_root()


def save_json(file_dir, data):
	with open(file_dir, 'w') as f:
		json.dump(data, f, sort_keys=True, indent=4)


def load_json(file_dir):
	try:
		with open(file_dir, 'r') as read_file:
			return json.load(read_file)

	except FileNotFoundError:
		print(f'Failed to read data... Perform get_repos and assure data.json is in folder.')


def generate_new_header():
	global token_index
	new_header = {
		'Content-Type': 'application/json',
		'Authorization': f'bearer {TOKEN_LIST[token_index]}'
	}
	if token_index < len(TOKEN_LIST) - 1:
		token_index += 1
	else:
		token_index = 0
	return new_header


# Query to be made on the current test
def create_query(cursor, user_login):
	if cursor is None:
		cursor = 'null'

	else:
		cursor = '\"{}\"'.format(cursor)
	query = """
	{
		user(login: "%s") {
			repositoriesContributedTo(first:10, after:%s, includeUserRepositories: false) {
				totalCount
				nodes {
					databaseId
					primaryLanguage {
						name
					}
					stargazers {
						totalCount
					}
					commitComments {
						totalCount
					}
				}
				pageInfo {
					endCursor
					hasNextPage
				}
			}
		}
		rateLimit {
			remaining
		}
	}
	""" % (user_login, cursor)
	return query


def do_github_request():
	res = requests.post(
		f'{URL}',
		json={'query': create_query(cursor=rct_cursor, user_login=login)}, headers=headers)
	res.raise_for_status()
	return dict(res.json()), res


if __name__ == "__main__":
	print(f"\n**** Starting GitHub API Requests *****\n")
	locations_dir = f'{ROOT}/resources/refined_locations.json'
	data_log_dir = f"{ROOT}/data_files_test/data_log.csv"
	finished_rct_dir = f'{ROOT}/resources/finished_rct.json'
	finished_rcts = load_json(finished_rct_dir)
	locations = load_json(locations_dir)
	countries = locations.keys()
	user_count_dict = load_json(data_log_dir)
	token_index = 0
	headers = generate_new_header()
	for country in countries:
		print(f"\n**** Starting Country {country.upper()} Requests *****\n")
		users_csv_dir = f"{ROOT}/data_files_test/{country}/{country}_users.csv"
		repos_csv_dir = f"{ROOT}/data_files_test/{country}/{country}_repos.csv"
		try:
			users = pd.read_csv(users_csv_dir)
			repos_df = pd.read_csv(repos_csv_dir)
			for row in users.itertuples():
				# row[n] => csv 'position'
				index = row[0]
				user_databaseId = row[1]
				login = row[8]
				if user_databaseId in finished_rcts[country]:
					print('Already mined data for this user...')

				else:
					total_pages = 0
					response = ''
					repos_has_next_page = True
					rct_cursor = None
					remaining_nodes = 666  # random number to initialize remaining_nodes
					page_counter = 0
					is_new_page = True

					while repos_has_next_page:
						print('Working on repositories contributed to...')
						try:
							if remaining_nodes < 200:
								print('Changing GitHub Access Token...')
								headers = generate_new_header()

							# rct => short for repositoriesContributedTo
							rct_data, response = do_github_request()

							if 'errors' in rct_data.keys():
								repos_has_next_page = False
								break

							if response.status_code == 200:
								total_pages = rct_data['data']['user']['repositoriesContributedTo']['totalCount']
								rct_cursor = rct_data['data']['user']['repositoriesContributedTo']['pageInfo']['endCursor']
								repos_has_next_page = rct_data['data']['user']['repositoriesContributedTo']['pageInfo']['hasNextPage']
								remaining_nodes = rct_data['data']['rateLimit']['remaining']
								for rct in rct_data['data']['user']['repositoriesContributedTo']['nodes']:
									# if rct['databaseId'] not in repos_df['repository_databaseId'].values:
									# if str(user_databaseId) + str(rct['databaseId']) not in repos_df['key'].values:
									cleaned_data = dict()
									cleaned_data['key'] = str(user_databaseId) + str(rct['databaseId'])
									cleaned_data['user_databaseId'] = user_databaseId
									cleaned_data['repository_databaseId'] = rct['databaseId']
									if rct['primaryLanguage'] is not None:
										cleaned_data['language'] = rct['primaryLanguage']['name']
									else:
										cleaned_data['language'] = 'Undefined language'

									cleaned_data['commitComments'] = rct['commitComments']['totalCount']
									cleaned_data['stars'] = rct['stargazers']['totalCount']
									print(f"New REPO with ID {cleaned_data['repository_databaseId']} found")
									repos_df = repos_df.append(cleaned_data, ignore_index=True)

							if not repos_has_next_page:
								print('Changing to next repository...')
								rct_cursor = None

						except requests.exceptions.ConnectionError:
							print(f'Connection error during the request')

						except requests.exceptions.HTTPError:
							print(f'HTTP request error... Sleeping 10 seconds to retry')
							sleep(10)

						except KeyboardInterrupt:
							repos_df.to_csv(repos_csv_dir, index=False, header=True)

						finally:
							print('Completed repos {}/{} of user {} ({}/{})'.format(
								page_counter, total_pages, login, index + 1, len(users)))
							repos_df.to_csv(repos_csv_dir, index=False, header=True)
							page_counter += 10

					finished_rcts[country].append(user_databaseId)
					save_json(file_dir=finished_rct_dir, data=finished_rcts)

		except FileNotFoundError:
			print(f'{users_csv_dir} not found...')
