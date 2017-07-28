import datetime


def start_message(step_name):
    s = datetime.datetime.now()
    print('-' * 80)
    print(step_name)
    print('Start: ', s)
    return s


def finish_message(start_time):
    finish_time = datetime.datetime.now()
    print('Finish: ', finish_time)
    print('Delta: ', finish_time - start_time)
    print('-' * 80)
    return finish_time
