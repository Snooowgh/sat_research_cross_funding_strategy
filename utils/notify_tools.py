# coding=utf-8
"""
@Project     : darwin_core_plus
@Author      : Arson
@File Name   : notify_tools
@Description :
@Time        : 2023/11/27 19:53
"""
import datetime
import json
import logging
import sys
import time

import asyncio
import requests
from loguru import logger
from notifiers.logging import NotificationHandler

from config.env_config import env_config
from utils.decorators import call_rate_limit, async_me
from utils.time_utils import get_datetime_now_str
from telegram import Bot
from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException

from logging import Handler, LogRecord
from collections import defaultdict
import time

"""
    Slack App管理
    https://api.slack.com/apps/A064ZET2CFL
"""


# Twilio 配置
TWILIO_ACCOUNT_SID = "ACxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"  # 替换为你的Account SID
TWILIO_AUTH_TOKEN = "your_auth_token"  # 替换为你的Auth Token
TWILIO_PHONE_NUMBER = "+1234567890"  # 替换为你的Twilio电话号码
ADMIN_PHONE_NUMBER = "+8613800138000"  # 替换为管理员电话号码


class CHANNEL_TYPE:
    QUANT_SIGNAL = "quant-signal"
    QUIET = "arb-log"
    MARKET = "market-signal"

    TRADE = "trade-notify"
    DEV = "dev-notify"

    CEX_QUIET = "key-arb-log"
    FUND_INVESTOR = "fund-investor"

    CHAIN_MARKET = "chain-market"
    LIQUIDATE_MARKET = "liquidate-market"


class SmartErrorHandler(Handler):
    def __init__(self, enable_phone_notification=False):
        super().__init__()
        self.error_counter = defaultdict(int)
        self.last_alert = 0
        self.alert_interval = 60 * 60  # 60分钟报警间隔
        self.enable_phone_notification = enable_phone_notification

    def emit(self, record: LogRecord):
        error_key = f"{record.pathname}:{record.lineno}"
        if error_key not in self.error_counter or self.error_counter[error_key] < 3:
            self.notify(record)
        self.error_counter[error_key] += 1

        # 频率控制报警
        current_time = time.time()
        if (current_time - self.last_alert) > self.alert_interval:
            top_errors = sorted(self.error_counter.items(),
                                key=lambda x: x[1],
                                reverse=True)[:3]
            self.send_digest(top_errors)
            self.last_alert = current_time
            self.error_counter.clear()

    def notify(self, record):
        # 实际应使用sentry_sdk
        # print(f"Sentry记录：{error_type} - {error_msg}")
        message = ""
        message += f'🚨 错误告警 🚨\n'
        message += f'时间：{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n'
        message += f'级别：{record.levelname}\n'
        message += f'位置： {record.pathname} {record.module}:{record.lineno} {record.funcName}\n'
        try:
            short_msg = record.msg.split("\n\n")[-1]
            message += f'内容：\n{short_msg}\n'
        except:
            short_msg = record.msg
            message += f'内容：\n\n{short_msg}\n'
        # message += f'内容：\n\n{record.msg}\n'

        notify_telegram(message)

        # 对于CRITICAL级别的错误，发送电话通知
        if record.levelname == "CRITICAL" and self.enable_phone_notification:
            phone_msg = f"系统严重错误：{record.pathname} 第{record.lineno}行。{short_msg[:100]}"
            send_sms_notification(phone_msg, urgent=True)

    def send_digest(self, top_errors):
        """发送错误摘要"""
        report = "📊 系统错误摘要\n"
        for (location, count) in top_errors:
            report += f"位置：{location} 次数：{count}\n"
        notify_telegram(report)  # 替换为实际通知逻辑


def send_urgent_notification(message, channel_type=CHANNEL_TYPE.TRADE,
                             enable_telegram=True, enable_slack=False,
                             enable_sms=False, enable_voice_call=False,
                             phone_number=None):
    """
    发送紧急通知，支持多种通知渠道

    Args:
        message (str): 通知内容
        channel_type: 通知频道类型
        enable_telegram (bool): 是否发送Telegram通知
        enable_slack (bool): 是否发送Slack通知
        enable_sms (bool): 是否发送短信通知
        enable_voice_call (bool): 是否拨打电话通知
        phone_number (str): 电话号码，如果为None则使用默认号码

    Returns:
        dict: 各个通知渠道的发送结果
    """
    results = {
        'telegram': False,
        'slack': False,
        'sms': False,
        'voice_call': False
    }

    # 发送Telegram通知
    if enable_telegram:
        try:
            notify_telegram(message, channel_type=channel_type)
            results['telegram'] = True
        except Exception as e:
            logger.error(f"Telegram通知发送失败: {e}")

    # 发送Slack通知
    if enable_slack:
        try:
            send_slack_message(message, channel_type=channel_type)
            results['slack'] = True
        except Exception as e:
            logger.error(f"Slack通知发送失败: {e}")

    # 发送短信通知
    if enable_sms:
        try:
            results['sms'] = send_sms_notification(message, phone_number=phone_number, urgent=True)
        except Exception as e:
            logger.error(f"短信通知发送失败: {e}")

    # 拨打电话通知
    if enable_voice_call:
        try:
            results['voice_call'] = send_voice_call_notification(message, phone_number=phone_number, urgent=True)
        except Exception as e:
            logger.error(f"电话通知发送失败: {e}")

    return results


def init_logger(directory=None, enable_phone_notification=False):
    """
    初始化日志系统

    Args:
        directory (str): 日志文件存储目录
        enable_phone_notification (bool): 是否启用电话通知功能
    """
    if directory is None:
        directory = "./logs"
    logger.remove()
    logger.add(sys.stderr, level="INFO")

    # defaults = {
    #     'webhook_url': SLACK_DEV_URL,
    # }
    # hdlr = NotificationHandler('slack', defaults=defaults)

    hdlr = SmartErrorHandler(enable_phone_notification=enable_phone_notification)
    logger.add(hdlr, level="ERROR")
    logger.add(directory + "/info.log", rotation="100 MB",
               level="INFO", encoding='utf-8')
    logger.add(directory + "/error.log", rotation="100 MB",
               level="ERROR", encoding='utf-8')
    logger.add(directory + "/debug.log", rotation="100 MB",
               level="DEBUG", encoding='utf-8')


def config_debug_logger():
    logger.remove()
    logger.add(sys.stderr, level="DEBUG")


# 定义发送消息的函数
def send_sms_notification(message, phone_number=None, urgent=False):
    """
    发送短信通知

    Args:
        message (str): 短信内容
        phone_number (str): 接收电话号码，如果为None则使用默认管理员号码
        urgent (bool): 是否为紧急消息，紧急消息会有特殊标记

    Returns:
        bool: 发送是否成功
    """
    if not message:
        return False

    # 检查配置是否完整
    if (TWILIO_ACCOUNT_SID.startswith("ACx") or
            TWILIO_AUTH_TOKEN == "your_auth_token" or
            TWILIO_PHONE_NUMBER.startswith("+1234567890")):
        logger.warning("Twilio配置不完整，无法发送短信通知")
        return False

    target_phone = phone_number or ADMIN_PHONE_NUMBER

    # 添加紧急标记
    if urgent:
        message = "🚨【紧急】" + message

    # 限制短信长度
    if len(message) > 1600:  # SMS标准限制
        message = message[:1600 - 3] + "..."

    try:
        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

        # 发送短信
        message_obj = client.messages.create(
            body=message,
            from_=TWILIO_PHONE_NUMBER,
            to=target_phone
        )

        logger.info(f"短信发送成功，SID: {message_obj.sid}, 电话: {target_phone}")
        return True

    except TwilioRestException as e:
        logger.error(f"短信发送失败 (Twilio错误): {e}")
        return False
    except Exception as e:
        logger.error(f"短信发送失败 (系统错误): {e}")
        return False


def send_voice_call_notification(message, phone_number=None, urgent=False):
    """
    发送语音电话通知（使用Twilio的TTS功能）

    Args:
        message (str): 语音播报内容
        phone_number (str): 接收电话号码，如果为None则使用默认管理员号码
        urgent (bool): 是否为紧急呼叫

    Returns:
        bool: 发送是否成功
    """
    if not message:
        return False

    # 检查配置是否完整
    if (TWILIO_ACCOUNT_SID.startswith("ACx") or
            TWILIO_AUTH_TOKEN == "your_auth_token" or
            TWILIO_PHONE_NUMBER.startswith("+1234567890")):
        logger.warning("Twilio配置不完整，无法拨打电话")
        return False

    target_phone = phone_number or ADMIN_PHONE_NUMBER

    # 添加紧急标记
    if urgent:
        message = "紧急通知。" + message

    # 限制语音消息长度，避免超时
    if len(message) > 200:
        message = message[:200 - 10] + "等。详情请查看Telegram或Slack通知。"

    try:
        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

        # 使用TTS进行语音播报
        call = client.calls.create(
            twiml=f'<Response><Say voice="alice" language="zh-CN">{message}</Say></Response>',
            from_=TWILIO_PHONE_NUMBER,
            to=target_phone
        )

        logger.info(f"语音电话呼叫成功，SID: {call.sid}, 电话: {target_phone}")
        return True

    except TwilioRestException as e:
        logger.error(f"语音电话呼叫失败 (Twilio错误): {e}")
        return False
    except Exception as e:
        logger.error(f"语音电话呼叫失败 (系统错误): {e}")
        return False


@call_rate_limit(30, 10, False)
def send_slack_message(message, channel_type=CHANNEL_TYPE.TRADE, image_buffer_map=None):
    if not message:
        return
    # notify_dingding(message, channel_type=channel_type)
    notify_telegram(message, channel_type=channel_type)
    return
    # SLACK_DEV_URL = "https://hooks.slack.com/services/xxx/xxx/xxx"
    # SLACK_FILE_UPLOAD_URL = 'https://slack.com/api/files.upload'
    # SLACK_TOKEN = "xoxp-xxxx"
    # logger.info("slack message paused..")
    # logger.info(message)
    # return
    # if len(message) > 1024:
    #     message = message[:1024] + "..."
    if not message and image_buffer_map is None:
        return
    if image_buffer_map is None:
        image_buffer_map = {}
    if channel_type == CHANNEL_TYPE.TRADE:
        url = "https://hooks.slack.com/services/xxx/xx/xx"
        logger.info(f"{channel_type}:\n{message}")
    elif channel_type == CHANNEL_TYPE.MARKET:
        url = "https://hooks.slack.com/services/xxx/xx/xx"
    else:
        raise Exception("not defined channel")
    if message:
        ret = requests.post(url, json={
            "text": message
        }, headers={"Content-type": "application/json"}).content
        if ret != b'ok':
            logger.info(f"send slack message failed, ret: {ret}")
            logger.info(message)
    if not image_buffer_map:
        return
    image_url_map = {}
    for indx, (name, image_buffer) in enumerate(image_buffer_map.items()):
        filename = str(indx) + "-" + str(int(time.time())) + ".png"
        files = {
            'file': (filename, image_buffer, 'image/png')
        }
        # 附加请求参数，确保上传成功
        data = {
            'channels': channel_type,  # 指定接收图片的频道
            'filename': filename,
            'token': SLACK_TOKEN
        }
        # 上传图片到 Slack
        try:
            response = requests.post(SLACK_FILE_UPLOAD_URL, files=files, data=data)
        except Exception as e:
            logger.warning(f"Error uploading image to Slack: {e}")
            continue
        # 检查响应结果
        if response.status_code != 200 or not response.json().get('ok'):
            logger.error(f"Error uploading image to Slack: {response.text}")
            continue
        # 返回上传成功后生成的文件ID
        image_url = response.json()['file']['permalink']
        image_url_map[name] = image_url
        time.sleep(0.5)
    if image_url_map:
        payload = {
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"<{image_url}|{name}>"
                    },
                }
                for indx, (name, image_url) in enumerate(image_url_map.items())
            ]
        }
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        # 发送消息到 Slack 频道
        response = requests.post(url, headers=headers, data=json.dumps(payload))

        # 检查响应状态
        if response.status_code != 200:
            logger.error(f"Error sending message to Slack: {response.text}")


def notify_dingding(content="", image_urls=None, debug=False, channel_type=CHANNEL_TYPE.DEV):
    if not content:
        return
    if image_urls is None:
        image_urls = []
    logger.info(channel_type + "\n" + content)
    # 请求的URL，WebHook地址
    content = content.replace("\n", "\n\n")
    # content += "\n\n触发时间:\n{}\n".format(get_datetime_now_str())
    chain_token = "xxxxx"
    strategy_token = "xxxxx"
    market_token = "xxxxx"
    important_token = "xxxxx"
    if channel_type in [CHANNEL_TYPE.TRADE, CHANNEL_TYPE.DEV]:
        token = important_token
    elif channel_type in [CHANNEL_TYPE.CEX_QUIET]:
        token = strategy_token
    elif channel_type in [CHANNEL_TYPE.MARKET, CHANNEL_TYPE.LIQUIDATE_MARKET]:
        token = market_token
    elif channel_type in [CHANNEL_TYPE.CHAIN_MARKET]:
        token = chain_token
    else:
        notify_telegram(content, channel_type=channel_type)
        return
    # 匹配关键词才能触发
    # if level == "normal":
    #     token = normal_notify_token
    #     title = "[Order]" + title
    # else:
    #     token = error_notify_token
    #     title = "[Error]" + title
    webhook = "https://oapi.dingtalk.com/robot/send?access_token=" + token
    header = {
        "Content-Type": "application/json",
        "Charset": "UTF-8"
    }
    infos = content.split("\n")
    title = infos[0] if len(infos) > 0 else content
    text = content
    if isinstance(image_urls, list):
        for image_url in image_urls:
            text += "![image](%s)\n\n" % image_url
    message = {
        "msgtype": "markdown",
        "markdown": {
            "title": title,
            "text": text
        },
        "at": {
            "isAtAll": True
        }
    }
    message_json = json.dumps(message)
    info = requests.post(url=webhook, data=message_json, headers=header).json()
    if debug:
        print(info)
    if info["errcode"] == 0:
        logger.info("[钉钉提醒]{}".format(title))
    else:
        notify_telegram(content, channel_type=channel_type)
    return 1


def notify_telegram(content="", image_urls=None, debug=False, channel_type=CHANNEL_TYPE.DEV, photo=None,
                    chat_ids=None):
    if not content:
        return
    # 请求的URL，WebHook地址
    notify_mark = env_config.get_str("NOTIFY_MARK", "")
    content = content + "\n\n 📢 " + channel_type + " " + notify_mark
    if chat_ids is None:
        chat_ids = env_config.get("TELEGRAM_BOT_CHAT_ID", "")
    async def send_telegram_notification(
            message: str,
            token,
            parse_mode: str = None,
            disable_web_page_preview: bool = False,
            disable_notification: bool = False
    ):

        async def send_msg(msg):
            ret = None
            for chat_id in chat_ids.split(","):
                ret = await bot.send_message(
                    chat_id=chat_id,
                    text=msg,
                    parse_mode=parse_mode,
                    disable_web_page_preview=disable_web_page_preview,
                    disable_notification=disable_notification
                )
            return ret

        try:
            bot = Bot(token=token)

            result = None
            if message:
                if len(message) < 4000:
                    result = await send_msg(message)
                else:
                    splited_text = message.split("\n\n")
                    if len(splited_text) == 1:
                        split_indx = message[:int(len(message) / 2)].rindex("\n")
                        splited_text = [message[:split_indx], message[split_indx:]]
                    for info in splited_text:
                        if len(info) > 4000:
                            for info_1 in [info[:len(info) // 2], info[len(info) // 2:]]:
                                await send_msg(info_1 + "\n")
                        elif len(info.strip()) > 0:
                            await send_msg(info + "\n")
            if photo:
                for chat_id in chat_ids.split(","):
                    result = await bot.send_photo(
                        chat_id=chat_id,
                        photo=photo
                    )
            return True if result else False
        except Exception as e:
            logger.warning(f"发送Telegram消息失败: {e}")
            return False

    if channel_type in [CHANNEL_TYPE.QUIET, CHANNEL_TYPE.CEX_QUIET]:
        # sat_research_bot (quiet) 静音频道
        token = env_config.get("TELEGRAM_BOT_TOKEN_LOG")
    else:
        # sat_research_alert_bot
        token = env_config.get("TELEGRAM_BOT_TOKEN_ALERT")
    asyncio.run(send_telegram_notification(content, token))


async def async_notify_telegram(content="", image_urls=None, debug=False, channel_type=CHANNEL_TYPE.DEV, photo=None,
                                chat_ids=None):
    if not content:
        return
    # 请求的URL，WebHook地址
    notify_mark = env_config.get_str("NOTIFY_MARK", "")
    content = content + "\n\n 📢 " + channel_type + " " + notify_mark
    if chat_ids is None:
        chat_ids = env_config.get("TELEGRAM_BOT_CHAT_ID", "")

    async def send_telegram_notification(
            message: str,
            token,
            parse_mode: str = None,
            disable_web_page_preview: bool = False,
            disable_notification: bool = False
    ):

        async def send_msg(msg):
            ret = None
            for _chat_id in chat_ids.split(","):
                ret = await bot.send_message(
                        chat_id=_chat_id,
                        text=msg,
                        parse_mode=parse_mode,
                        disable_web_page_preview=disable_web_page_preview,
                        disable_notification=disable_notification
                )
            return ret

        try:
            bot = Bot(token=token)

            result = None
            if message:
                if len(message) < 4000:
                    result = await send_msg(message)
                else:
                    splited_text = message.split("\n\n")
                    if len(splited_text) == 1:
                        split_indx = message[:int(len(message) / 2)].rindex("\n")
                        splited_text = [message[:split_indx], message[split_indx:]]
                    for info in splited_text:
                        if len(info) > 4000:
                            for info_1 in [info[:len(info) // 2], info[len(info) // 2:]]:
                                await send_msg(info_1 + "\n")
                        elif len(info.strip()) > 0:
                            await send_msg(info + "\n")
            if photo:
                for chat_id in chat_ids.split(","):
                    result = await bot.send_photo(
                        chat_id=chat_id,
                        photo=photo
                    )
            return True if result else False
        except Exception as e:
            logger.warning(f"发送Telegram消息失败: {e}")
            return False

    if channel_type in [CHANNEL_TYPE.QUIET, CHANNEL_TYPE.CEX_QUIET]:
        # sat_research_bot (quiet) 静音频道
        token = env_config.get_str("TELEGRAM_BOT_TOKEN_LOG")
    else:
        # sat_research_alert_bot
        token = env_config.get_str("TELEGRAM_BOT_TOKEN_ALERT")
    await send_telegram_notification(content, token)


def run_phone_notifications():
    """
    测试电话通知功能
    """
    # 测试短信通知
    test_message = "这是一条测试短信，用于验证短信通知功能是否正常工作。"
    sms_result = send_sms_notification(test_message, urgent=False)
    logger.info(f"短信测试结果: {sms_result}")

    # 测试紧急短信通知
    urgent_message = "这是一条紧急测试短信，包含紧急标记。"
    urgent_sms_result = send_sms_notification(urgent_message, urgent=True)
    logger.info(f"紧急短信测试结果: {urgent_sms_result}")

    # 测试语音电话通知
    voice_message = "这是一条测试语音通知，系统运行正常。"
    voice_result = send_voice_call_notification(voice_message, urgent=False)
    logger.info(f"语音电话测试结果: {voice_result}")

    # 测试综合紧急通知
    urgent_notification = "系统检测到严重异常，需要立即处理！"
    comprehensive_result = send_urgent_notification(
        urgent_notification,
        enable_telegram=True,
        enable_slack=False,
        enable_sms=True,
        enable_voice_call=False  # 设为False避免实际拨打电话
    )
    logger.info(f"综合紧急通知测试结果: {comprehensive_result}")


if __name__ == '__main__':
    # 原有测试
    asyncio.run(async_notify_telegram("Test1123123", channel_type=CHANNEL_TYPE.TRADE,chat_ids="1237945663,1009361554"))
    # notify_telegram("Test1123123", channel_type=CHANNEL_TYPE.TRADE,chat_ids="1237945663,1009361554")
    # asyncio.run(async_notify_telegram("Test1", channel_type=CHANNEL_TYPE.QUIET))
    # notify_telegram("Test2")

    # 新增电话通知测试（需要先配置正确的Twilio参数）
    # test_phone_notifications()
