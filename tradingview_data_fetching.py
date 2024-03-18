# %%
import pytz
import yaml
import json
import holidays
import pendulum
import tradingview_ta
from pathlib import Path
from loguru import logger
from cloudpathlib import S3Path
from result import Err, Ok, is_err
from datetime import datetime, timedelta
from utils.logger import configure_logger
from dateutil.relativedelta import relativedelta, MO
from utils.constants import pair_list, interval_list
from utils.delete_local_data_and_log_from_logger import (
    delete_local_data,
    delete_all_local_data,
)


# %%
def create_local_logger(local_log_path_name: str, logger_name: str):
    datetime_now = pendulum.now("Asia/Hong_Kong").strftime("%Y-%m-%d-%H-%M-%S")
    configure_logger(local_log_path_name, f"{logger_name}_{datetime_now}")
    logger.success("Logger configured successfully")


# %%
def config_pair(target_pair_list: list) -> Ok[list] | Err[str]:
    try:
        if target_pair_list[0] == "ALL":
            target_pair_list = ["FX_IDC:" + pair for pair in pair_list]
            return Ok(target_pair_list)
        elif target_pair_list[0] != "ALL":
            # Check if all elements in target_pair_list are in pair_list list, avoid misspelling
            is_all_in_pair_list = all(item in pair_list for item in target_pair_list)
            unmatched_items = [
                item for item in target_pair_list if item not in pair_list
            ]
            if is_all_in_pair_list is False:
                return Err(
                    f"Error in configuring pair, unmatched items found: {unmatched_items}"
                )
            target_pair_list = ["FX_IDC:" + pair for pair in target_pair_list]
            return Ok(target_pair_list)
    except Exception as e:
        return Err(f"Error in configuring pair: {e}")


# %%
def config_interval(target_interval_list: list) -> Ok[list] | Err[str]:
    try:
        if target_interval_list[0] == "ALL":
            target_interval_list = interval_list
            return Ok(target_interval_list)
        elif target_interval_list[0] != "ALL":
            # Check if all elements in interval_list are in interval list, avoid misspelling
            is_all_in_interval_list = all(
                item in interval_list for item in target_interval_list
            )
            unmatched_items = [
                item for item in target_interval_list if item not in interval_list
            ]
            if is_all_in_interval_list is False:
                return Err(
                    f"Error in configuring time_interval, unmatched items found: {unmatched_items}"
                )
            target_interval_list = target_interval_list
            return Ok(target_interval_list)
    except Exception as e:
        return Err(f"Error in configuring time_interval: {e}")


# %%
def datetime_handler(target_interval_list: list) -> Ok[dict] | Err[str]:
    israel_timezone = pytz.timezone("Israel")
    israel_now = datetime.now(israel_timezone)
    current_minute = israel_now.minute
    current_hour = israel_now.hour
    current_date = israel_now.date()

    country_code = "US"
    public_holidays = holidays.CountryHoliday(country_code)

    datetime_dict = {}
    try:
        for interval in target_interval_list:
            if interval.endswith("m"):
                interval_value = int(interval[:-1])
                rounded_value = (current_minute // interval_value) * interval_value
                # Format the rounded value with leading zeros if necessary
                formatted_rounded_value = "{:02d}".format(rounded_value)
                formatted_datetime = israel_now.strftime(
                    "%Y-%m-%d-%H-{}-00".format(formatted_rounded_value)
                )
                datetime_dict[interval] = formatted_datetime

            elif interval.endswith("h"):
                interval_value = int(interval[:-1])
                rounded_value = (current_hour // interval_value) * interval_value
                formatted_rounded_value = "{:02d}".format(rounded_value)
                formatted_datetime = israel_now.strftime(
                    "%Y-%m-%d-{}-00-00".format(formatted_rounded_value)
                )
                datetime_dict[interval] = formatted_datetime

            elif interval.endswith("d"):
                while current_date in public_holidays:
                    current_date += timedelta(days=1)
                formatted_datetime = "{}-00-00-00".format(current_date)
                datetime_dict[interval] = formatted_datetime

            elif interval.endswith("W"):
                # Pass MO(-1) as an argument to relativedelta to set weekday as Monday and -1 signifies last week's Monday
                current_monday = current_date + relativedelta(weekday=MO(-1))
                # Check if the Monday date is international public holiday (i.e. US)
                while current_monday in public_holidays:
                    current_monday += timedelta(days=1)
                formatted_datetime = "{}-00-00-00".format(current_monday)
                datetime_dict[interval] = formatted_datetime

            elif interval.endswith("M"):
                first_day_of_current_month = current_date + relativedelta(day=1)
                while first_day_of_current_month in public_holidays:
                    first_day_of_current_month += timedelta(days=1)
                formatted_datetime = "{}-00-00-00".format(first_day_of_current_month)
                datetime_dict[interval] = formatted_datetime
        return Ok(datetime_dict)

    except Exception as e:
        return Err(f"Error when handling the datetime: {e}")


# %%
def fetch_technical_analysis_summary(
    screener: str, target_interval_list: list, target_pair_list: list
) -> Ok[dict] | Err[str]:
    try:
        # add each pair as a key to the dictionary with an empty dictionary first
        technical_analysis_summary_dict = {}
        for tradingview_symbol in target_pair_list:
            # e.g. extract "AUDCAD" from "FX_IDC:AUDCHF" for the key of the dict
            pair = tradingview_symbol.split(":")[1]
            technical_analysis_summary_dict[pair] = {}

        # for interval and pair, fetch data
        for interval in target_interval_list:
            technical_analysis_summary = tradingview_ta.get_multiple_analysis(
                screener=screener, interval=interval, symbols=target_pair_list
            )
            for tradingview_symbol in target_pair_list:
                pair = tradingview_symbol.split(":")[1]
                technical_analysis_summary_dict[pair].update(
                    {interval: technical_analysis_summary[tradingview_symbol].summary}
                )
        return Ok(technical_analysis_summary_dict)
    except Exception as e:
        return Err(f"Error in fetching technical analysis summary: {e}")


# %%
def fetch_technical_indicators(
    screener: str, target_interval_list: list, target_pair_list: list
) -> Ok[dict] | Err[str]:
    try:
        technical_indicators_dict = {}
        for tradingview_symbol in target_pair_list:
            # e.g. extract "AUDCAD" from "FX_IDC:AUDCHF" for the key of the dict
            pair = tradingview_symbol.split(":")[1]
            technical_indicators_dict[pair] = {}

        #  for interval and pair, fetch data
        for interval in target_interval_list:
            technical_indicators = tradingview_ta.get_multiple_analysis(
                screener=screener, interval=interval, symbols=target_pair_list
            )
            for tradingview_symbol in target_pair_list:
                # e.g. extract "AUDCAD" from "FX_IDC:AUDCHF" for the key of the dict
                pair = tradingview_symbol.split(":")[1]
                technical_indicators_dict[pair].update(
                    {interval: technical_indicators[tradingview_symbol].indicators}
                )
        return Ok(technical_indicators_dict)
    except Exception as e:
        return Err(f"Error in fetching technical analysis summary: {e}")


# %%
def get_combined_dict(
    technical_analysis_summary: dict, technical_indicators: dict
) -> dict:
    dict1 = technical_analysis_summary
    dict2 = technical_indicators
    combined_dict = dict1
    for outer_pair_key, outer_dict_value in dict2.items():
        for inner_interval_key, inner_dict_value in outer_dict_value.items():
            for key, value in inner_dict_value.items():
                combined_dict[outer_pair_key][inner_interval_key][key] = value
    return combined_dict


# %%
def save_dict_as_json_to_local_file(
    combined_dict: dict, local_data_base_path: Path, datetime_dict: dict
) -> Ok[str] | Err[str]:
    for outer_pair_key, outer_dict_value in combined_dict.items():
        for inner_interval_key, inner_dict_value in outer_dict_value.items():
            local_output_path = local_data_base_path / outer_pair_key
            local_output_path.mkdir(parents=True, exist_ok=True)
            file_name = f"{outer_pair_key}_{inner_interval_key}_{datetime_dict[inner_interval_key]}.json"
            local_file_path = local_output_path / file_name

            with open(local_file_path, "w") as json_file:
                json.dump(
                    {outer_pair_key: {inner_interval_key: inner_dict_value}}, json_file
                )
            if not local_file_path.exists():
                return Err(f"{local_file_path} does not exist")

    return Ok("All data was outputted to JSON files and saved in the local folder")


# %%
def generate_key(filename: str) -> str:
    pair, interval, date = filename.split("_")
    year, month, day, hour, minute, second = date.split("-")
    return f"{interval}/{year}/{month}/{day}/{hour}/{filename}"


# %%
def upload_local_data_to_s3(
    target_pair_list: list,
    local_data_base_path: Path,
    s3_output_base_path: S3Path,
) -> Ok[str] | Err[str]:
    try:
        for tradingview_symbol in target_pair_list:
            pair = tradingview_symbol.split(":")[1]
            local_file_path = local_data_base_path / pair
            for path in local_file_path.glob("**/*.json"):
                filename = path.name
                s3_key = generate_key(filename)
                s3_output_data_path = s3_output_base_path / pair / s3_key
                s3_output_data_path.upload_from(path)
                logger.success(
                    f"Successfully uploaded {local_file_path} to {s3_output_data_path}"
                )
    except Exception as e:
        return Err(f"Error uploading {local_file_path} to {s3_output_data_path}: {e}")
    return Ok("Finish uploading")


# %%
def upload_latest_log_to_s3(
    local_log_path: Path, s3_output_base_path: S3Path
) -> Ok[str] | Err[str]:
    log_files = list(local_log_path.glob("*"))
    # get the latest log file and upload it, avoid upload irrelevant log files
    # st_mtime represents the time of the last modification of the file in seconds
    latest_log_file = max(log_files, key=lambda f: f.stat().st_mtime)
    s3_upload_path = s3_output_base_path / "log" / latest_log_file.name
    try:
        s3_upload_path.upload_from(str(latest_log_file))
        return Ok(f"{latest_log_file} uploaded to {s3_output_base_path}")
    except Exception as e:
        return Err(f"Error uploading {latest_log_file} to {s3_output_base_path}: {e}")


# %%
def read_variable_from_config_file(config) -> list and str and S3Path and Path:
    target_pair_list = config["target_pair_list"]
    target_interval_list = config["target_interval_list"]
    tradingview_data_fetching_job_config = config["tradingview_data_fetching_job"]
    job_name = tradingview_data_fetching_job_config["job_name"]
    s3_output_base_path = S3Path(
        tradingview_data_fetching_job_config["s3_output_base_path"]
    )
    local_data_base_path = Path(
        tradingview_data_fetching_job_config["local_data_base_path"]
    )
    return (
        target_pair_list,
        target_interval_list,
        job_name,
        s3_output_base_path,
        local_data_base_path,
    )


# %%
def tradingview_data_fetching_job(config_file_name_str: str) -> Ok[str] | Err[str]:
    with open(f"./config/{config_file_name_str}", "r") as file:
        config = yaml.safe_load(file)

    # read variable from config file
    (
        target_pair_list,
        target_interval_list,
        job_name,
        s3_output_base_path,
        local_data_base_path,
    ) = read_variable_from_config_file(config)
    logger.info(f"The name of this job: {job_name}")
    logger.info(f"Configuration file name is {config_file_name_str}")
    logger.info(f"Local TradingView data path is ./{local_data_base_path}")
    logger.info(f"S3 TradingView data path is {s3_output_base_path}")

    # config logger object
    local_log_path_name = f"{job_name}_logs"
    local_log_path = Path(local_log_path_name)
    create_local_logger(local_log_path_name, job_name)
    logger.info(f"The local log path is ./{local_log_path}")

    # config pair
    config_pair_result = config_pair(target_pair_list)
    if is_err(config_pair_result):
        logger.error(config_pair_result.err_value)
        return Err(config_pair_result.err_value)
    target_pair_list = config_pair_result.ok_value
    logger.success(f"The target pair list for this job is: {target_pair_list}")

    # config time interval
    config_interval_result = config_interval(target_interval_list)
    if is_err(config_interval_result):
        logger.error(config_interval_result.err_value)
        return Err(config_interval_result.err_value)
    target_interval_list = config_interval_result.ok_value
    logger.success(f"The target interval list for this job is: {target_interval_list}")

    # generate related datetime for naming the file
    datetime_handler_result = datetime_handler(target_interval_list)
    if is_err(datetime_handler_result):
        logger.error(datetime_handler_result.err_value)
        return Err(datetime_handler_result.err_value)
    datetime_dict = datetime_handler_result.ok_value
    logger.success(f"The target datetime for this job is: {datetime_dict}")

    # delete all data before data fetching if old data have not been deleted
    if local_data_base_path.exists():
        logger.info("Cleared the tradingview data folder before new data comes in")
        delete_local_data(local_data_base_path)

    # fetch technical analysis summary for each pair and each time interval
    logger.info(
        "Start fetching technical analysis summary for each pair and each time interval"
    )
    fetch_technical_analysis_summary_result = fetch_technical_analysis_summary(
        "forex", target_interval_list, target_pair_list
    )
    if is_err(fetch_technical_analysis_summary_result):
        logger.error(fetch_technical_analysis_summary_result.err_value)
        upload_latest_log_to_s3(local_log_path, s3_output_base_path)
        return Err(fetch_technical_analysis_summary_result.err_value)
    logger.success("Fetched all target technical analysis summary")
    technical_analysis_summary_dict = fetch_technical_analysis_summary_result.ok_value

    # fetch technical indicators for each pair and each time interval
    logger.info(
        "Start fetching technical indicators for each pair and each time interval"
    )
    fetch_technical_indicators_result = fetch_technical_indicators(
        "forex", target_interval_list, target_pair_list
    )
    if is_err(fetch_technical_indicators_result):
        logger.error(fetch_technical_indicators_result.err_value)
        upload_latest_log_to_s3(local_log_path, s3_output_base_path)
        return Err(fetch_technical_indicators_result.err_value)
    logger.success("Fetched all target technical indicators")
    technical_indicators_dict = fetch_technical_indicators_result.ok_value

    # combined these two dictionaries
    combined_dict = get_combined_dict(
        technical_analysis_summary_dict, technical_indicators_dict
    )

    # for the value of each pair and each time interval in the combined dict, save it as json file to local folder
    logger.info(
        "For the value of each pair and each time interval in the combined dict, start transforming it into json file"
    )
    save_dict_as_json_to_local_file_result = save_dict_as_json_to_local_file(
        combined_dict, local_data_base_path, datetime_dict
    )
    if is_err(save_dict_as_json_to_local_file_result):
        logger.error(save_dict_as_json_to_local_file_result.err_value)
        upload_latest_log_to_s3(local_log_path, s3_output_base_path)
        return Err(save_dict_as_json_to_local_file_result.err_value)
    logger.success(save_dict_as_json_to_local_file_result.ok_value)

    # upload data to S3
    logger.info("Start uploading data from local to S3 bucket")
    upload_local_data_to_s3_result = upload_local_data_to_s3(
        target_pair_list,
        local_data_base_path,
        s3_output_base_path,
    )
    if is_err(upload_local_data_to_s3_result):
        logger.error(upload_local_data_to_s3_result.err_value)
        return Err(upload_local_data_to_s3_result.err_value)
    logger.success(upload_local_data_to_s3_result.ok_value)

    # upload log to S3
    logger.info("Start uploading current log file from local to S3 bucket")
    upload_latest_log_to_s3_result = upload_latest_log_to_s3(
        local_log_path, s3_output_base_path
    )
    if is_err(upload_latest_log_to_s3_result):
        logger.error(upload_latest_log_to_s3_result.err_value)
        return Err(upload_latest_log_to_s3_result.err_value)
    logger.success(upload_latest_log_to_s3_result.ok_value)
    logger.success("Finish TradingView data fetching job")

    # delete all data
    delete_all_local_data(local_data_base_path, local_log_path)

    return Ok(f"Finish TradingView data fetching job")
