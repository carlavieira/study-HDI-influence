import os
import json
from time import sleep

import requests
import pandas as pd
import random
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()


def get_project_root() -> Path:
	return Path(__file__).parent.parent


def save_json(file_dir, data):
	with open(file_dir, 'w') as f:
		json.dump(data, f, sort_keys=True, indent=4)


URL = 'https://api.github.com/graphql'
TOKEN_LIST = json.loads(os.getenv("GITHUB_ACCESS_TOKENS"))
random.shuffle(TOKEN_LIST)
ROOT = get_project_root()


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
			issues(first: 20, after:%s ,orderBy: {field: CREATED_AT, direction: ASC}, filterBy: {since: "2020-01-01T00:00:00Z"}) {	
				pageInfo {
					endCursor
					hasNextPage
				}
				totalCount
				nodes {
					createdAt
					databaseId
					state
					closed
					closedAt
					repository {
						name
						owner {
							login
						}
					}
				}
			}
		}
		rateLimit {
			remaining
		}
	}
	""" % (user_login, cursor)
	return query


def calculate_duration(date_creation, date_action):
	date_time_obj_start = datetime.strptime(date_creation, "%Y-%m-%dT%H:%M:%SZ")
	date_time_obj_end = datetime.strptime(date_action, "%Y-%m-%dT%H:%M:%SZ")
	duration_in_s = (date_time_obj_end - date_time_obj_start).total_seconds()
	return round(duration_in_s / 3600)


def do_github_request():
	res = requests.post(
		f'{URL}',
		json={'query': create_query(cursor=issues_cursor, user_login=login)},
		headers=headers
	)
	res.raise_for_status()
	return dict(res.json()), res


def save_clean_data(dataframe):
	date_limit = datetime.strptime("2020-12-31T23:59:00Z", "%Y-%m-%dT%H:%M:%SZ")
	for issue in issues_data['data']['user']['issues']['nodes']:
		if datetime.strptime(issue['createdAt'], "%Y-%m-%dT%H:%M:%SZ") <= date_limit:
			# if issue is not None and issue['databaseId'] not in issues_df['issue_databaseId'].values:
			cleaned_data = dict()
			cleaned_data['user_databaseId'] = user_databaseId
			cleaned_data['issue_databaseId'] = issue['databaseId']
			cleaned_data['repo_owner'] = issue['repository']['owner']['login']
			cleaned_data['repo_name'] = issue['repository']['name']
			cleaned_data['state'] = issue['state']
			cleaned_data['createdAt'] = issue['createdAt']
			cleaned_data['closed'] = issue['closed']
			cleaned_data['closedAt'] = issue['closedAt']

			if cleaned_data['state'] == 'CLOSED':
				if cleaned_data['closedAt']:
					cleaned_data['duration'] = calculate_duration(cleaned_data['createdAt'], cleaned_data['closedAt'])

			print(f"New ISSUE with ID {cleaned_data['issue_databaseId']} found")
			dataframe = dataframe.append(cleaned_data, ignore_index=True)

	return dataframe


if __name__ == "__main__":
	print(f"\n**** Starting GitHub API Requests *****\n")
	locations_dir = f'{ROOT}/resources/refined_locations.json'
	finished_issues_dir = f'{ROOT}/resources/finished_issues.json'
	finished_issues = load_json(finished_issues_dir)
	locations = load_json(locations_dir)
	countries = locations.keys()
	token_index = 0
	headers = generate_new_header()
	for country in countries:
		print(f"\n**** Starting Country {country.upper()} Requests *****\n")
		users_csv_dir = f"{ROOT}/data_files_test/{country}/{country}_users.csv"
		issues_csv_dir = f"{ROOT}/data_files_test/{country}/{country}_issues.csv"
		try:
			users = pd.read_csv(users_csv_dir)
			issues_df = pd.read_csv(issues_csv_dir)
			for row in users.itertuples():
				# row[n] => csv 'position'
				index = row[0]
				user_databaseId = row[1]
				login = row[8]

				if user_databaseId in finished_issues[country]:
					print('Already mined data for this user...')

				else:
					total_pages = 0
					response = ''
					issues_has_next_page = True
					issues_cursor = None
					remaining_nodes = 666  # random number to initialize remaining_nodes
					page_counter = 0

					while issues_has_next_page:
						print('Working on ISSUES...')
						try:
							if remaining_nodes < 200:
								print('Changing GitHub Access Token...')
								headers = generate_new_header()

							issues_data, response = do_github_request()
							if 'errors' in issues_data.keys():
								print(issues_data)
								issues_has_next_page = False
								break

							if response.status_code == 200:
								total_pages = round(issues_data['data']['user']['issues']['totalCount'] / 10 + 0.5)
								issues_cursor = issues_data['data']['user']['issues']['pageInfo']['endCursor']
								issues_has_next_page = issues_data['data']['user']['issues']['pageInfo']['hasNextPage']
								remaining_nodes = issues_data['data']['rateLimit']['remaining']

								issues_df = save_clean_data(dataframe=issues_df)
								if not issues_has_next_page:
									print('Changing to next ISSUE...')
									issues_cursor = None

						except requests.exceptions.ConnectionError:
							print(f'Connection error during the request')

						except requests.exceptions.HTTPError:
							print(f'HTTP request error... Sleeping 10 seconds to retry')
							sleep(10)

						except KeyboardInterrupt:
							issues_df.to_csv(issues_csv_dir, index=False, header=True)

						finally:
							print('Completed issues {}/{} of user {} ({}/{})'.format(
								page_counter, total_pages, login, index + 1, len(users)))
							issues_df.to_csv(issues_csv_dir, index=False, header=True)
							page_counter += 1

					finished_issues[country].append(user_databaseId)
					save_json(file_dir=finished_issues_dir, data=finished_issues)

		except FileNotFoundError:
			print(f'{users_csv_dir} not found...')
