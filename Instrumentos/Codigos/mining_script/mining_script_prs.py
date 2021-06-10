import os
import json
from time import sleep
import requests
import pandas as pd
import random
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

num_nodes_request = 10

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
		print(f'Failed to read data...')


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
			name
			pullRequests(first: 10, after:%s, orderBy: {field: CREATED_AT, direction: DESC}) {
				totalCount
				pageInfo {
					hasNextPage
					endCursor
				}
				nodes {
					additions
					body
					deletions
					databaseId
					state
					createdAt
					closedAt
					state
					merged
					mergedAt
					commits(first: 1) {
						totalCount
					}
					editor {
						login
					}
					author {
						login
					}
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
	return round(duration_in_s / 60)


def do_github_request():
	res = requests.post(
		f'{URL}',
		json={'query': create_query(cursor=pr_cursor, user_login=login)},
		headers=headers
	)
	res.raise_for_status()
	return dict(res.json()), res


def save_clean_data(dataframe):
	date_within_limit = True
	for pr in pr_data['data']['user']['pullRequests']['nodes']:

		cleaned_data = dict()
		cleaned_data['user_databaseId'] = user_databaseId
		cleaned_data['pull_request_databaseId'] = pr['databaseId']
		cleaned_data['repo_owner'] = pr['repository']['owner']['login']
		cleaned_data['repo_name'] = pr['repository']['name']
		cleaned_data['state'] = pr['state']
		cleaned_data['createdAt'] = pr['createdAt']
		cleaned_data['additions'] = pr['additions']
		cleaned_data['deletions'] = pr['deletions']
		cleaned_data['closedAt'] = pr['closedAt']
		cleaned_data['merged'] = pr['merged']
		cleaned_data['mergedAt'] = pr['mergedAt']
		cleaned_data['body'] = len(pr['body'])
		cleaned_data['author'] = pr['author']['login']
		cleaned_data['totalLines'] = cleaned_data['additions'] + cleaned_data['deletions']
		cleaned_data['totalCommits'] = pr['commits']['totalCount']
		if pr['editor'] is not None:
			cleaned_data['editor'] = pr['editor']['login']
		else:
			cleaned_data['editor'] = 'Undefined editor'
		# for commit in pr['commits']['nodes']:
		# 	print('{} -> {}'.format(commit['commit']['committer']['name'], pr_data['data']['user']['name']))
		# checar se commit percente ao user

		if datetime.strptime(cleaned_data['createdAt'], "%Y-%m-%dT%H:%M:%SZ") < date_limit:
			date_within_limit = False

		if cleaned_data['state'] == 'MERGED':
			if cleaned_data['mergedAt']:
				cleaned_data['duration_merged'] = calculate_duration(cleaned_data['createdAt'], cleaned_data['mergedAt'])

		else:
			if cleaned_data['closedAt']:
				cleaned_data['duration_closed'] = calculate_duration(cleaned_data['createdAt'], cleaned_data['closedAt'])

		print(f"New PR with ID {cleaned_data['pull_request_databaseId']} found")
		dataframe = dataframe.append(cleaned_data, ignore_index=True)

	return dataframe, date_within_limit and prs_has_next_page


if __name__ == "__main__":
	print(f"\n**** Starting GitHub API Requests *****\n")
	locations_dir = f'{ROOT}/resources/refined_locations.json'
	finished_prs_dir = f'{ROOT}/resources/finished_prs.json'
	finished_prs = load_json(finished_prs_dir)
	locations = load_json(locations_dir)
	countries = locations.keys()
	token_index = 0
	headers = generate_new_header()
	date_limit = datetime(2020, 4, 24)
	for country in countries:
		print(f"\n**** Starting Country {country.upper()} Requests *****\n")
		users_csv_dir = f"{ROOT}/data_files_test/{country}/{country}_users.csv"
		prs_csv_dir = f"{ROOT}/data_files_test/{country}/{country}_prs.csv"

		# print(f"\n**** Starting Country IN Requests *****\n")
		# users_csv_dir = f"{ROOT}/data_files_test/in/in_users.csv"
		# prs_csv_dir = f"{ROOT}/data_files_test/in/in_prs.csv"

		try:
			users = pd.read_csv(users_csv_dir)
			prs_df = pd.read_csv(prs_csv_dir)
			for row in users.itertuples():
				created_date_out_of_limit = False
				# row[n] => csv 'position'
				index = row[0]
				user_databaseId = row[1]
				login = row[8]

				if user_databaseId in finished_prs['in']:
					print('Already mined data for this user...')

				else:
					total_pages = 0
					response = ''
					prs_has_next_page = True
					pr_cursor = None
					remaining_nodes = 666  # random number to initialize remaining_nodes
					page_counter = 0
					change_query = False  # need this?

					while prs_has_next_page:
						print('Working on PULL REQUESTS...')
						try:
							if remaining_nodes < 200:
								print('Changing GitHub Access Token...')
								headers = generate_new_header()

							pr_data, response = do_github_request()

							if 'errors' in pr_data.keys():
								print(pr_data)
								issues_has_next_page = False
								break

							if response.status_code == 200:
								total_pages = round(pr_data['data']['user']['pullRequests']['totalCount'] / 10 + 0.5)
								pr_cursor = pr_data['data']['user']['pullRequests']['pageInfo']['endCursor']
								prs_has_next_page = pr_data['data']['user']['pullRequests']['pageInfo']['hasNextPage']
								remaining_nodes = pr_data['data']['rateLimit']['remaining']
								prs_df, prs_has_next_page = save_clean_data(prs_df)

								if not prs_has_next_page:
									print('Changing to next PR...')
									pr_cursor = None

						except requests.exceptions.ConnectionError:
							print(f'Connection error during the request')

						except requests.exceptions.HTTPError:
							print(f'HTTP request error... Sleeping 10 seconds to retry')
							sleep(10)

						except KeyboardInterrupt:
							prs_df.to_csv(prs_csv_dir, index=False, header=True)

						finally:
							print('Completed PRs {}/{} of user {} ({}/{})'.format(
								page_counter, total_pages, login, index + 1, len(users)))

							prs_df.to_csv(prs_csv_dir, index=False, header=True)
							page_counter += 1

				finished_prs['in'].append(user_databaseId)
				save_json(file_dir=finished_prs_dir, data=finished_prs)

		except FileNotFoundError:
			print(f'{users_csv_dir} not found...')
