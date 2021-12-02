#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint

import slack
from slack import WebClient

from nludialog.core.dmo import AbacusSlackBot
from nludialog.core.dmo import EventMessageParser
from nludialog.core.dmo import SlackResponseFormatter
from nludialog.core.svc import PerformResponse


def _has_subtype(an_event: dict) -> bool:
    if 'subtype' in an_event:
        return True
    if 'data' in an_event:
        if 'subtype' in an_event['data']:
            return True
    return False


def _subtype(an_event: dict) -> str:
    if 'subtype' in an_event:
        return an_event['subtype']
    if 'data' in an_event:
        if 'subtype' in an_event['data']:
            return an_event['data']['subtype']


def _chit_chat_response(parser: EventMessageParser,
                        web_client: WebClient,
                        flow_name: str):
    user = parser.user()
    thread_ts = parser.ts()
    channel_id = parser.channel()

    dialog = PerformResponse()
    text_response = dialog.process(flow_name).nlg()

    slack_response = SlackResponseFormatter().process(user, text_response)

    web_client.chat_postMessage(
        channel=channel_id,
        text=slack_response,
        thread_ts=thread_ts)


def _command_router(parser: EventMessageParser,
                    web_client: WebClient,
                    command_flow: str):
    if command_flow != "COMMAND_RELATED_ENTITIES":
        raise NotImplementedError("\n".join([
            f"Unsupported Command Flow (name={command_flow})"]))




def main():
    from nluflows import AbacusMappingAPI
    from datamongo import BaseMongoClient
    from datamongo import SlackEventCollection

    mapping_api = AbacusMappingAPI(is_debug=True)

    base_mongo_client = BaseMongoClient()
    slack_event_collection = SlackEventCollection(base_mongo_client=base_mongo_client,
                                                  collection_name="event_slack")

    def _bot_message(payload: dict):
        pass

    def _user_message(payload: dict):
        parser = EventMessageParser(payload)
        slack_event_collection.save(parser.event)

        web_client = parser.web_client()
        rtm_client = parser.rtm_client()

        result_mapping = mapping_api.prediction(min_confidence=80,
                                                trigger_ts=parser.ts()).process(parser.text())

        result_analysis = mapping_api.analysis(result_mapping).is_chit_chat()
        if result_analysis[0]:
            _chit_chat_response(parser, web_client, result_analysis[1])
        else:

            result_analysis = mapping_api.analysis(result_mapping).is_command()
            if result_analysis[0]:
                _command_router(parser, web_client, result_analysis[1])

    @slack.RTMClient.run_on(event='message')
    def say_hello(**payload):

        print('******************************************************')
        pprint.pprint(payload)
        print('******************************************************')

        if _has_subtype(payload) and _subtype(payload) == 'bot_message':
            _bot_message(payload)
        else:
            _user_message(payload)

    slackbot = AbacusSlackBot()


if __name__ == "__main__":
    import plac

plac.call(main)
