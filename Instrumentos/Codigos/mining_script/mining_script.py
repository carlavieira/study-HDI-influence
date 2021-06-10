import os
import json
import requests
import pandas as pd
import random

from dotenv import load_dotenv
from pathlib import Path

#
# from resources.utils import get_project_root

num_nodes_request = 10

load_dotenv()


def get_project_root() -> Path:
	return Path(__file__).parent.parent


URL = 'https://api.github.com/graphql'
TOKEN_LIST = json.loads(os.getenv("GITHUB_ACCESS_TOKENS"))
random.shuffle(TOKEN_LIST)
ROOT = get_project_root()


def save_json(file_dir):
	with open(file_dir, 'w') as f:
		json.dump(user_count_dict, f, sort_keys=True, indent=4)


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
def create_query(cursor=None):
	if cursor is None:
		cursor = 'null'

	else:
		cursor = '\"{}\"'.format(cursor)
	query = """
	query github {
		search (query: "followers:0..%s location:%s",  type: USER, first: %s, after: %s) {
			userCount
			pageInfo {
				endCursor
				hasNextPage
					}
			nodes {
				... on User {
					databaseId
					login
					location
					followers {
						totalCount
					}
					commitComments {
						totalCount
					}
					issues {
						totalCount
					}
					pullRequests {
						totalCount
					}
					repositoriesContributedTo {
						totalCount
					}
				}
			}
		}
		rateLimit {
			remaining
		}
	}
	""" % (max_followers, location, str(num_nodes_request), cursor)
	print(cursor)
	return query


def create_new_country_dir():
	files = ['repos', 'users']
	new_dir = f"{ROOT}/data_files_test/{country}"
	os.mkdir(new_dir)
	for file in files:
		write_path = f'{new_dir}/{country}_{file}.csv'
		mode = 'a' if os.path.exists(write_path) else 'w'
		with open(write_path, mode) as f:
			if file == 'users':
				f.write('databaseId,location,commitComments,issues,pullRequests,repositoriesContributedTo\n')
			else:
				f.write('user_databaseId, language, stars\n')


if __name__ == "__main__":
	print(f"\n**** Starting GitHub API Requests *****\n")
	locations_dir = f'{ROOT}/resources/refined_locations.json'
	data_log_dir = f"{ROOT}/data_files_test/data_log.csv"
	locations = load_json(locations_dir)
	countries = locations.keys()
	user_count_dict = load_json(data_log_dir)
	token_index = 0
	headers = generate_new_header()
	remaining_nodes = 5000
	for country in countries:
		print(f"\n**** Starting Country {country.upper()} Requests *****\n")
		users_csv_dir = f"{ROOT}/data_files_test/{country}/{country}_users.csv"

		try:
			users = pd.read_csv(users_csv_dir)
		except FileNotFoundError:
			print(f'{users_csv_dir} not found! Creating the directory and files...')
			create_new_country_dir()
		finally:
			users = pd.read_csv(users_csv_dir)

		for location in locations[country]:
			if user_count_dict[country][location]['total_count'] == 0:
				print(f"Locations for {country.upper()} : {locations[country]}")
				print(f"\n**** Starting Location {location} Requests *****\n")
				user_count = None
				has_next_page = True
				last_cursor = None
				page_counter = 0
				is_new_page = True
				query_remaining = 1
				change_query = False
				max_followers = "9"
				nodes_counter = 0
				target = 1
				while nodes_counter < target:
					page_counter += 1
					condition = True
					while condition:
						try:
							if remaining_nodes < 200:
								print('Changing GitHub Access Token...')
								headers = generate_new_header()

							# if user_count:
							# 	query_remaining = round(user_count / 1000 + 0.5) - 1

							if page_counter != 1 and page_counter % (1000 // num_nodes_request) == 1:
								# noinspection PyUnboundLocalVariable
								print(f"Changing query to user with last than {last_followers} followers")
								max_followers = last_followers - 1
								last_cursor = None

							response = requests.post(f'{URL}', json={'query': create_query(last_cursor)}, headers=headers)
							response.raise_for_status()
							data = dict(response.json())

							if not user_count_dict[country][location]['total_count']:
								user_count = data['data']['search']['userCount']
								target = round(user_count * 0.1 + 0.5)
								user_count_dict[country][location]['total_count'] = user_count
								user_count_dict[country][location]['target'] = target
							last_cursor = data['data']['search']['pageInfo']['endCursor']
							has_next_page = data['data']['search']['pageInfo']['hasNextPage']
							remaining_nodes = data['data']['rateLimit']['remaining']

							for d in data['data']['search']['nodes']:
								if d:
									if d['databaseId'] not in users['databaseId'].values and nodes_counter < target:
										s = pd.Series([
											d['databaseId'], d['login'], d['location'], d['commitComments']['totalCount'],
											d['issues']['totalCount'], d['pullRequests']['totalCount'],
											d['repositoriesContributedTo']['totalCount'], d['followers']['totalCount']],
											index=[
												'databaseId', 'login', 'location', 'commitComments', 'issues',
												'pullRequests', 'repositoriesContributedTo', 'followers'])
										users = users.append(s, ignore_index=True)
										nodes_counter += 1
										print(f"[Country: {country.upper()}] [Location:{location}] {nodes_counter}/{target} found ID: {d['databaseId']} Followers: {d['followers']['totalCount']}")
									else:
										print("User already in database")
									last_followers = d['followers']['totalCount']

						except requests.exceptions.ConnectionError:
							print(f'Connection error during the request')

						except requests.exceptions.HTTPError:
							# noinspection PyUnboundLocalVariable
							print(f'HTTP request error. STATUS: {response.status_code}')
							headers = generate_new_header()
							users.to_csv(users_csv_dir, index=False, header=True)
							print(last_cursor)

						except requests.exceptions.ChunkedEncodingError:
							print('A bizarre error occurred...')

						except FileNotFoundError:
							print(f'File not found.')

						except KeyboardInterrupt:
							users.to_csv(users_csv_dir, index=False, header=True)
							print(last_cursor)

						else:
							if user_count:
								print(f"Page {page_counter} succeeded! {nodes_counter*100 // target}% of users")
							condition = False

				users.to_csv(users_csv_dir, index=False, header=True)
				save_json(data_log_dir)
