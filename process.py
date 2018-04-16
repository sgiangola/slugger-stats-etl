import argparse
import process_games
import process_players
import process_game_stats
import data_utils
from datetime import datetime


def main(args):
    if not args.date:
        date = datetime.now()
    else:
        date = datetime.strptime(args.date, '%Y-%m-%d')
    # if there are games unfinished that are final but have not been processed
    # fetch them to get their data (eg. game ended after 12pm EST)
    unfinished_games = process_games.get_unfinished_games()
    # get all games and their status for a given date and insert to db
    process_games.process(date)
    if args.game_meta_only:
        return
    # get games that have not been processed for stats
    games_stats = data_utils.get_unprocessed_games_stats()
    # process stats
    if games_stats:
        run_stats(games_stats)
    else:
        print('No new games to process')
    if unfinished_games:
        run_stats(unfinished_games)
        print('Updated {count} games'.format(count=len(unfinished_games)))
    else:
        print('No games to update')
    if args.player or args.all:
        # get games that have not been processed for players
        games_players = data_utils.get_unprocessed_games_player()
        # process players
        if games_players:
            run_players(games_players)
        else:
            print('No unprocessed games for players')


def run_stats(games):
    stats_separate = [
        process_game_stats.parse_stats(game)
        for game in games
        ]
    stats = data_utils.join_units(stats_separate)
    process_game_stats.upsert(stats)
    data_utils.mark_games_processed(games, 'stats')


def run_players(games):
    players_separate = [
        process_players.parse_players(game) for game in games
        ]
    players = data_utils.join_units(players_separate)
    process_players.player_insert_and_update(players)
    data_utils.mark_games_processed(games, 'players')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--date', help='date to run', type=str)
    parser.add_argument('-g', '--game_meta_only',
                        help='only process game metadata',
                        action="store_true")
    parser.add_argument('-a', '--all', help='run all etl', action='store_true')
    parser.add_argument('-b', '--batting',
                        help='run etl for player_batting',
                        action='store_true')
    parser.add_argument('-p', '--player',
                        help='run etl for player dimension',
                        action='store_true')
    main(parser.parse_args())
