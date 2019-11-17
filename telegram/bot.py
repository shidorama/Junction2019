"""
1062873478:AAGMRl_3cd3_zjAnI13vdQs6mUoshio-3uU
"""
import ast
import logging
import math
from datetime import datetime, timedelta

import numpy as np
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CommandHandler, CallbackContext, MessageHandler, Filters, CallbackQueryHandler

trials = [
    [912, "Kattila niityn portin k\u00e4vij\u00e4laskuri", 24.495358359272675, 60.32692454295634,
     "Other (big parking place)", "Easy", None, None, 1, 1, 1, 1, 1, 1, 1, 0, 1],
    [922, "Nuuksio Takalanpolun laskuri", 24.49283804648889, 60.33073081360451, "Takalan polku", "Easy", "3",
     50.0, 1, 1, 1, 1, 1, 1, 1, 0, 1],
    [1043, "Haukkalampi - Solvalla yhdysreitin k\u00e4vij\u00e4laskuri", 24.55039100896953, 60.303525451006834,
     "Yhdysreitti Haukkalampi-Haltia", "Extreme", "4,6", 120.0, 0, 0, 1, 0, 1, 1, 1, 0, 1],
    [1050, "Nuuksio, Veikkolan k\u00e4vij\u00e4laskuri", 24.463689284278523, 60.27581963490531, "Kaarniaispolku",
     "Easy", "2,7", 60.0, 0, 0, 0, 0, 0, 0, 1, 0, 1],
    [1225, "Mustakorventien 2. laskuri", 24.456289749063266, 60.29819039594651, "Other short track", None, "3",
     None, 0, 0, 0, 0, 0, 0, 1, 0, 0],
    [1246, "Siikaranta k\u00e4vij\u00e4laskuri", 24.50587378959941, 60.282421996333504, "Soidinkierros", "Easy",
     "4", 120.0, 0, 0, 0, 0, 0, 0, 0, 1, 1]
]

route_ids = [912, 922, 1043, 1050, 1225, 1246]
forecast_data = []


def load_predictions():
    with open("daily_prediction.csv") as fp:
        for line in fp:
            prediction = line.split(',')
            trial_id = int(prediction[0])
            if trial_id in route_ids:
                forecast_data.append(
                    (trial_id, 952, float(prediction[3]), datetime.strptime(prediction[1], "%Y-%m-%d")))


load_predictions()

# for trial_id in route_ids:
#     for x in range(0, 7):
#         forecast_data.append(
#             (trial_id, 952, math.ceil(random() * 100) / 100,
#              datetime.now().replace(minute=0, hour=0, second=0, microsecond=0) + timedelta(days=x))
#         )


updater = Updater(token='', use_context=True)
dispatcher = updater.dispatcher

parks = ["Nuuksio", "Pallas-yllas"]
parks_id = {952: "Nuuksio", 34361: "Pallas-yllas"}
parks_name_to_id = {"Nuuksio": 952, "Pallas-yllas": 34361}
park_kb_markup = InlineKeyboardMarkup(
    [[InlineKeyboardButton(v, callback_data='{"info": True, "park_id": %s}' % k) for k, v in parks_id.items()]],
    resize_keyboard=True, one_time_keyboard=True)


def start(update: Update, context: CallbackContext):
    print("got start")

    # markup = ReplyKeyboardMarkup([parks], resize_keyboard=True, one_time_keyboard=True)
    context.bot.send_message(update.effective_chat.id, parse_mode="Markdown",
                             text="***Hello!***\nWelcome to park info bot!\nPlease select park to proceed",
                             reply_markup=park_kb_markup)
    # context.bot.send_location(update.effective_chat.id, 60.326924542956334, 24.495358359272675)
    # context.bot.sendVenue(update.effective_chat.id, 0, 0, "almost correct place")
    # bot.send_message(chat_id=update.message.chat_id, text="hello")


def full_park_menu_init(update: Update, context: CallbackContext, park_id: int):
    keyboard = [
        [
            InlineKeyboardButton("now", callback_data='{"forecast": "now", "park_id": %s}' % park_id),
            InlineKeyboardButton("1d", callback_data='{"forecast": "1", "park_id": %s}' % park_id),
            InlineKeyboardButton("2d", callback_data='{"forecast": "2", "park_id": %s}' % park_id),
            InlineKeyboardButton("3d", callback_data='{"forecast": "3", "park_id": %s}' % park_id)

        ],
        [
            InlineKeyboardButton("4d", callback_data='{"forecast": "4", "park_id": %s}' % park_id),
            InlineKeyboardButton("5d", callback_data='{"forecast": "5", "park_id": %s}' % park_id),
            InlineKeyboardButton("6d", callback_data='{"forecast": "6", "park_id": %s}' % park_id),
            InlineKeyboardButton("7d", callback_data='{"forecast": "7", "park_id": %s}' % park_id)

        ],
        [
            InlineKeyboardButton("Anytime but QUIET", callback_data='{"forecast": "quiet", "park_id": %s}' % park_id),
            InlineKeyboardButton("Anytime but BUSTLING",
                                 callback_data='{"forecast": "bustling", "park_id": %s}' % park_id),
        ],
        [
            InlineKeyboardButton("END", callback_data='{"forecast": "end", "park_id": %s}' % park_id),
        ]

    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    message = get_current_park_state(park_id) + "\n Usually we're really busy around 11-12!"
    context.bot.send_message(update.effective_chat.id, text=message, reply_markup=reply_markup)


def handle_generic_command(update: Update, context: CallbackContext):
    cmd = update.message.text
    # if cmd in parks:
    #     full_park_menu_init(update, context)
    if cmd[:4].lower() == "stop":
        parsed_cmd = cmd.split(" ")
        entr_id = int(parsed_cmd[1])
        for entrance in trials:
            if entrance[0] == entr_id:
                context.bot.send_location(update.effective_chat.id, entrance[3], entrance[2])
    # if cmd.lower() == 'kb':


def get_current_park_state(park_id):
    trials = []
    for forecast_row in forecast_data:
        if forecast_row[1] == park_id and forecast_row[3] == datetime.now().replace(minute=0, hour=0, second=0,
                                                                                    microsecond=0):
            trials.append(forecast_row[2])
    park_state = "unknown"
    if len(trials) == 0:
        return "We have no idea whats happening there.... ¯\_(ツ)_/¯"
    else:
        load = np.mean(trials)
        adj_load = math.ceil(load * 100)
        if load > 0.7:
            return "*Park is extremely busy now.* (%s) Please refrain from coming today if you can to preserve nature." % adj_load
        if load > 0.5:
            return "Park is very busy (%s). Some routes may be too busy to enjoy." % adj_load
        if load > 0.3:
            return "Park is moderately busy (%s)" % adj_load
        return "Park is almost empty! Best time to enjoy nature! (%s)" % adj_load


def get_routes_business(park_id, days):
    routes = {
        "avoid": [],
        "busy": [],
        "free": []
    }
    target_date = datetime.now().replace(minute=0, hour=0, second=0,
                                         microsecond=0) + timedelta(days=days)
    for forecast_row in forecast_data:
        if forecast_row[1] == park_id \
                and forecast_row[3] == target_date:
            load = forecast_row[2]
            if load > 0.7:
                routes["avoid"].append(forecast_row)
            elif load > 0.5:
                routes["busy"].append(forecast_row)
            else:
                routes["free"].append(forecast_row)

    return routes


def get_forecast_for(park_id, days):
    routes = get_routes_business(park_id, days)
    keyboard = [[],
                [InlineKeyboardButton("END", callback_data='{}')]]
    if len(routes["avoid"]) > 0:
        pass
    if len(routes["busy"]) > 0:
        keyboard[0].append(InlineKeyboardButton("Busy",
                                                callback_data='{"routes": "busy", "days": %s, "park_id": %s}' % (
                                                    days, park_id)))
    if len(routes["free"]) > 0:
        keyboard[0].append(InlineKeyboardButton("Free",
                                                callback_data='{"routes": "free", "days": %s, "park_id": %s}' % (
                                                    days, park_id)))
        message = """Currently there are:
+ %s routes you should avoid
+ %s routes that are busy
+ %s routes that are free""" % (len(routes["avoid"]), len(routes["busy"]), len(routes["free"]))

        return message, InlineKeyboardMarkup(keyboard)


def return_park_state_menu(update: Update, context: CallbackContext):
    context.bot.send_message(update.effective_chat.id, parse_mode="Markdown", text=get_current_park_state(0))


def get_routes_buttons(routes, days):
    keyboard = []
    for route in routes:
        route_id = route[0]
        trial = get_trial_by_id(route_id)
        keyboard.append(
            InlineKeyboardButton(trial[1], callback_data="{'specific_route': %s, 'days': %s}" % (route[0], days)))
    return InlineKeyboardMarkup([keyboard])


def get_trial_by_id(route_id):
    for trial in trials:
        if trial[0] == route_id:
            return trial


def handle_callback(update: Update, context: CallbackContext):
    query = update.callback_query
    bot = context.bot
    data = ast.literal_eval(update.callback_query.data)
    params = {"message_id": query.message.message_id,
              "chat_id": query.message.chat_id}
    if "forecast" in data:
        if data['forecast'] == 'end':
            params['text'] = 'Ok.'
            bot.edit_message_text(**params)
            bot.send_message(query.message.chat_id, text="Select park to continue", reply_markup=park_kb_markup)
        elif data['forecast'] == 'quiet':
            pass
        elif data['forecast'] == 'bustling':
            pass
        else:
            days = int(data['forecast'][:1])
            message, keyboard = get_forecast_for(data['park_id'], days)
            try:
                bot.send_message(query.message.chat_id, text=message, parse_mode="Markdown", reply_markup=keyboard)
            except Exception as e:
                print(e)
                raise e
            bot.delete_message(**params)

            # bot.answer_callback_query(callback_query_id=query.id, text=f"You sent a report")
    if "info" in data:
        park_id = data["park_id"]
        bot.delete_message(**params)
        full_park_menu_init(update, context, park_id)
    if 'routes' in data:
        routes = get_routes_business(data['park_id'], data['days'])
        keyboard = get_routes_buttons(routes[data['routes']], data['days'])
        bot.delete_message(**params)
        bot.send_message(query.message.chat_id, text="Select route", reply_markup=keyboard)
    if 'specific_route' in data:
        route = get_trial_by_id(data['specific_route'])
        bot.delete_message(**params)
        target_date = datetime.now().replace(minute=0, hour=0, second=0, microsecond=0) + timedelta(days=x)
        try:

            with open('./hists/%s_%s.png' % (route[0], target_date.weekday()), 'rb') as fp:
                context.bot.send_photo(update.effective_chat.id, fp, caption="Average route load for this day")
        except Exception as e:
            print(e)
            raise e
        context.bot.send_location(update.effective_chat.id, route[3], route[2])


dispatcher.add_handler(CommandHandler('start', start))
dispatcher.add_handler(MessageHandler(Filters.text, handle_generic_command))
dispatcher.add_handler(CallbackQueryHandler(handle_callback))

if __name__ == '__main__':
    log = logging.getLogger(__name__)
    log.info('Started bot')
    updater.start_polling()
