import json
import os
import time

import pandas as pd
from matplotlib import pyplot as plt

from src.database_connection import get_redis_connection

redis_conn = get_redis_connection()

select_durations = []
delete_durations = []
update_durations = []
insert_durations = []
count_durations = []
mean_durations = []
word_durations = []


def initialize_redis(data):
    pipe = redis_conn.pipeline()
    for index, row in data.iterrows():
        key = str(row['_id'])
        record = {
            'player_url': str(row['player_url']),
            'fifa_version': str(row['fifa_version']),
            'fifa_update': str(row['fifa_update']),
            'fifa_update_date': str(row['fifa_update_date']),
            'short_name': str(row['short_name']),
            'long_name': str(row['long_name']),
            'player_positions': str(row['player_positions']),
            'overall': str(row['overall']),
            'potential': str(row['potential']),
            'value_eur': str(row['value_eur']),
            'wage_eur': str(row['wage_eur']),
            'age': str(row['age']),
            'dob': str(row['dob']),
            'height_cm': str(row['height_cm']),
            'weight_kg': str(row['weight_kg']),
            'league_id': str(row['league_id']),
            'league_name': str(row['league_name']),
            'league_level': str(row['league_level']),
            'club_team_id': str(row['club_team_id']),
            'club_name': str(row['club_name']),
            'club_position': str(row['club_position']),
            'club_jersey_number': str(row['club_jersey_number']),
            'club_loaned_from': str(row['club_loaned_from']),
            'club_joined_date': str(row['club_joined_date']),
            'club_contract_valid_until_year': str(row['club_contract_valid_until_year']),
            'nationality_id': str(row['nationality_id']),
            'nationality_name': str(row['nationality_name']),
            'nation_team_id': str(row['nation_team_id']),
            'nation_position': str(row['nation_position']),
            'nation_jersey_number': str(row['nation_jersey_number']),
            'preferred_foot': str(row['preferred_foot']),
            'weak_foot': str(row['weak_foot']),
            'skill_moves': str(row['skill_moves']),
            'international_reputation': str(row['international_reputation']),
            'work_rate': str(row['work_rate']),
            'body_type': str(row['body_type']),
            'real_face': str(row['real_face']),
            'release_clause_eur': str(row['release_clause_eur']),
            'player_tags': str(row['player_tags']),
            'player_traits': str(row['player_traits']),
            'pace': str(row['pace']),
            'shooting': str(row['shooting']),
            'passing': str(row['passing']),
            'dribbling': str(row['dribbling']),
            'defending': str(row['defending']),
            'physic': str(row['physic']),
            'attacking_crossing': str(row['attacking_crossing']),
            'attacking_finishing': str(row['attacking_finishing']),
            'attacking_heading_accuracy': str(row['attacking_heading_accuracy']),
            'attacking_short_passing': str(row['attacking_short_passing']),
            'attacking_volleys': str(row['attacking_volleys']),
            'skill_dribbling': str(row['skill_dribbling']),
            'skill_curve': str(row['skill_curve']),
            'skill_fk_accuracy': str(row['skill_fk_accuracy']),
            'skill_long_passing': str(row['skill_long_passing']),
            'skill_ball_control': str(row['skill_ball_control']),
            'movement_acceleration': str(row['movement_acceleration']),
            'movement_sprint_speed': str(row['movement_sprint_speed']),
            'movement_agility': str(row['movement_agility']),
            'movement_reactions': str(row['movement_reactions']),
            'movement_balance': str(row['movement_balance']),
            'power_shot_power': str(row['power_shot_power']),
            'power_jumping': str(row['power_jumping']),
            'power_stamina': str(row['power_stamina']),
            'power_strength': str(row['power_strength']),
            'power_long_shots': str(row['power_long_shots']),
            'mentality_aggression': str(row['mentality_aggression']),
            'mentality_interceptions': str(row['mentality_interceptions']),
            'mentality_positioning': str(row['mentality_positioning']),
            'mentality_vision': str(row['mentality_vision']),
            'mentality_penalties': str(row['mentality_penalties']),
            'mentality_composure': str(row['mentality_composure']),
            'defending_marking_awareness': str(row['defending_marking_awareness']),
            'defending_standing_tackle': str(row['defending_standing_tackle']),
            'defending_sliding_tackle': str(row['defending_sliding_tackle']),
            'goalkeeping_diving': str(row['goalkeeping_diving']),
            'goalkeeping_handling': str(row['goalkeeping_handling']),
            'goalkeeping_kicking': str(row['goalkeeping_kicking']),
            'goalkeeping_positioning': str(row['goalkeeping_positioning']),
            'goalkeeping_reflexes': str(row['goalkeeping_reflexes']),
            'goalkeeping_speed': str(row['goalkeeping_speed']),
            'ls': str(row['ls']),
            'st': str(row['st']),
            'rs': str(row['rs']),
            'lw': str(row['lw']),
            'lf': str(row['lf']),
            'cf': str(row['cf']),
            'rf': str(row['rf']),
            'rw': str(row['rw']),
            'lam': str(row['lam']),
            'cam': str(row['cam']),
            'ram': str(row['ram']),
            'lm': str(row['lm']),
            'lcm': str(row['lcm']),
            'cm': str(row['cm']),
            'rcm': str(row['rcm']),
            'rm': str(row['rm']),
            'lwb': str(row['lwb']),
            'ldm': str(row['ldm']),
            'cdm': str(row['cdm']),
            'rdm': str(row['rdm']),
            'rwb': str(row['rwb']),
            'lb': str(row['lb']),
            'lcb': str(row['lcb']),
            'cb': str(row['cb']),
            'rcb': str(row['rcb']),
            'rb': str(row['rb']),
            'gk': str(row['gk']),
            'player_face_url': str(row['player_face_url'])
        }
        pipe.hmset(key, {str(k): str(v) for k, v in record.items()})

    pipe.execute()


def execute_delete(keys=""):
    if keys == "":
        keys = '*'
    start = time.time()
    for key in redis_conn.keys(keys):
        redis_conn.delete(key)
    end = time.time()
    delete_durations.append(end - start)


def execute_get_by_key(stmt):
    cols = ''.join(stmt.split()).split(';')
    start = time.time()
    key = cols[0]
    if len(cols) == 1:
        result = redis_conn.hgetall(int(key))
    else:
        name = cols[1]
        result = redis_conn.hget(key, name)
    end = time.time()
    select_durations.append(end - start)
    return result


def execute_insert(data):
    first_key = next(iter(json.loads(data)))
    key = data.pop(first_key)
    start = time.time()
    redis_conn.hmset(key, data)
    end = time.time()
    insert_durations.append(end - start)


def execute_update(data):
    first_key = next(iter(json.loads(data)))
    key = data.pop(first_key)
    start = time.time()
    redis_conn.hmset(key, data)
    end = time.time()
    update_durations.append(end - start)


def execute_count(stmt):
    start = time.time()
    res = pd.DataFrame()
    count = len(res.index)
    end = time.time()
    count_durations.append(end - start)
    plt.figure()
    plt.xlabel(res.columns[0])
    plt.ylabel("count")
    plt.hist(res[res.columns[0]])
    os.makedirs(os.path.dirname('results/'), exist_ok=True)
    plt.savefig('./results/hist_redis.png')
    return count


def execute_mean(stmt):
    start = time.time()
    res = pd.DataFrame()
    means, median = {}, {}
    for col in res.columns:
        means.update({col: res[col].mean()})
        median.update({col: res[col].median()})

    end = time.time()
    mean_durations.append(end - start)
    return means, median


def execute_word(stmt):
    stmt, new_values = stmt.split(';', 1)
    start = time.time()
    res = pd.DataFrame()
    amount = res[res.columns[0]].str.count(str(new_values)).sum()
    end = time.time()
    word_durations.append(end - start)
    return amount


def close_connection():
    redis_conn.close()
