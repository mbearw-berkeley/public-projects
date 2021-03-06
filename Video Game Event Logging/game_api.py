#!/usr/bin/env python
import json
from kafka import KafkaProducer
from flask import Flask, request

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:29092')


def log_to_kafka(topic, event):
    event.update(request.headers)
    producer.send(topic, json.dumps(event).encode())


@app.route("/")
def default_response():
    default_event = {'event_type': 'default'}
    log_to_kafka('events', default_event)
    return "This is the default response!\n"


@app.route("/purchase_a_sword")
def purchase_sword():
    purchase_sword_event = {'event_type': 'purchase_sword',
                           'description': "Traveler's_Sword"}
    log_to_kafka('events', purchase_sword_event)
    return "Traveler's Sword Purchased!\n"

@app.route("/purchase_a_blade")
def purchase_blade():
    purchase_blade_event = {'event_type': 'purchase_blade',
                           'description': 'Eightfold_Blade'}
    log_to_kafka('events', purchase_blade_event)
    return "Eightfold Blade Purchased!\n"

@app.route("/join_thieves_guild")
def join_thieves_guild():
    join_thieves_event = {"event_type": "join_guild",
                       'guild_type:':"thieves_guild",
                         "completed_missions":0}
    log_to_kafka("events",join_thieves_event)
    return "Joined Thieves Guild! \n"

@app.route("/join_mages_guild")
def join_mages_guild():
    join_mages_event = {"event_type": "join_guild",
                       'guild_type:':"mages_guild",
                       'completed_missions':0}
    log_to_kafka("events",join_mages_event)
    return "Joined Mages Guild!\n"