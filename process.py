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
    # get all games and their status for a given date and insert to db
    process_games.process(date)
    if args.game_meta_only:
        return
    # get games that have not been processed for stats
    games_stats = data_utils.get_unprocessed_games_stats()
    # process stats
    if games_stats:
        stats_separate = [
            process_game_stats.parse_stats(game)
            for game in games_stats
            ]
        stats = data_utils.join_units(stats_separate)
        process_game_stats.upsert(stats)
        data_utils.mark_games_processed(games_stats, 'stats')
    else:
        print('No unprocessed games for stats')
    if args.player or args.all:
        # get games that have not been processed for players
        games_players = data_utils.get_unprocessed_games_player()
        # process players
        if games_players:
            players_separate = [
                process_players.parse_players(game) for game in games_players
                ]
            players = data_utils.join_units(players_separate)
            process_players.player_insert_and_update(players)
            data_utils.mark_games_processed(games_players, 'players')
        else:
            print('No unprocessed games for players')


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
