# Copyright (c) 2018 The Regents of the University of Michigan
# and the University of Pennsylvania
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""
Utility functions for performing cross-validation for model training/testing.
"""

from morf.utils.log import set_logger_handlers
from morf.utils.config import MorfJobConfig
from morf.utils import fetch_courses, fetch_sessions, download_train_test_data, initialize_input_output_dirs, make_feature_csv_name, make_label_csv_name, clear_s3_subdirectory, make_s3_key_path, upload_file_to_s3, download_from_s3, initialize_labels
from morf.utils.s3interface import fetch_mode_files
from morf.utils.job_runner_utils import make_docker_run_command
from multiprocessing import Pool
import logging
import tempfile
import pandas as pd
import os
import numpy as np
from sklearn.model_selection import StratifiedKFold

module_logger = logging.getLogger(__name__)
CONFIG_FILENAME = "config.properties"



def aggregate_session_input_data(file_type, course, input_dir = "./input"):
    """
    Aggregate all csv data files matching pattern within input_dir (recursive file search), and write to a single file in input_dir.
    :param type: {"labels" or "features"}.
    :param dest_dir:
    :return:
    """
    valid_types = ("features", "labels")
    course_dir = os.path.join(input_dir, course)
    assert file_type in valid_types, "[ERROR] specify either features or labels as type."
    df_out = pd.DataFrame()
    for root, dirs, files in os.walk(course_dir, topdown=False):
        for session in dirs:
            session_csv = generate_archive_filename(job_config, mode=file_type, extension="csv")
            session_feats = os.path.join(root, session, session_csv)
            session_df = pd.read_csv(session_feats)
            df_out = pd.concat([df_out, session_df])
    # write single csv file
    if file_type == "features":
        outfile = make_feature_csv_name(course, file_type)
    elif file_type == "labels":
        outfile = "{}_{}.csv".format(course, file_type) #todo: use make_label_csv_name after updating that function
    outpath = os.path.join(input_dir, outfile)
    df_out.to_csv(outpath, index = False)
    return outpath


def create_course_folds(label_type, k = 5, multithread = True, raw_data_dir="morf-data/"):
    """
    From extract and extract-holdout data, create k randomized folds, pooling data by course (across sessions) and archive results to s3.
    :param label_type: type of outcome label to use.
    :param k: number of folds.
    :param multithread: logical indicating whether multiple cores should be used (if available)
    :param raw_data_dir: name of subfolder in s3 buckets containing raw data.
    :return:
    """
    user_id_col = "userID"
    label_col = "label_value"
    job_config = MorfJobConfig(CONFIG_FILENAME)
    mode = "cv"
    job_config.update_mode(mode)
    logger = set_logger_handlers(module_logger, job_config)
    # clear any preexisting data for this user/job/mode
    clear_s3_subdirectory(job_config)
    if multithread:
        num_cores = job_config.max_num_cores
    else:
        num_cores = 1
    logger.info("creating cross-validation folds")
    with Pool(num_cores) as pool:
        for raw_data_bucket in job_config.raw_data_buckets:
            for course in fetch_courses(job_config, raw_data_bucket):
                with tempfile.TemporaryDirectory(dir=job_config.local_working_directory) as working_dir:
                    input_dir, output_dir = initialize_input_output_dirs(working_dir)
                    # download data for each session
                    for session in fetch_sessions(job_config, raw_data_bucket, data_dir=raw_data_dir, course=course, fetch_all_sessions=True):
                        # get the session feature and label data
                        download_train_test_data(job_config, raw_data_bucket, raw_data_dir, course, session, input_dir, label_type=label_type)
                    # merge features to ensure splits are correct
                    feat_csv_path = aggregate_session_input_data(file_type="features", course=course,input_dir=input_dir)
                    label_csv_path = aggregate_session_input_data(file_type="labels", course=course,input_dir=input_dir)
                    feat_df = pd.read_csv(feat_csv_path, dtype=object)
                    label_df = pd.read_csv(label_csv_path, dtype=object)
                    feat_label_df = pd.merge(feat_df, label_df, on=user_id_col)
                    import ipdb;ipdb.set_trace()
                    assert feat_df.shape[0] == label_df.shape[0], "features and labels must contain same number of observations"
                    # create the folds
                    logger.info("creating cv splits with k = {} course {} session {}".format(k, course, session))
                    skf = StratifiedKFold(n_splits=k, shuffle=True)
                    folds = skf.split(np.zeros(feat_df.shape[0]), feat_label_df.label_value)
                    for fold_num, train_test_indices in enumerate(folds,1): # write each fold train/test data to csv and push to s3
                        train_index, test_index = train_test_indices
                        train_df,test_df = feat_label_df.loc[train_index,].drop(label_col, axis = 1), feat_label_df.loc[test_index,].drop(label_col, axis = 1)
                        train_df_name = os.path.join(working_dir, make_feature_csv_name(course, session, fold_num, "train"))
                        test_df_name = os.path.join(working_dir, make_feature_csv_name(course, session, fold_num, "test"))
                        train_df.to_csv(train_df_name, index = False)
                        test_df.to_csv(test_df_name, index=False)
                        # upload to s3
                        try:
                            train_key = make_s3_key_path(job_config, course, os.path.basename(train_df_name), session)
                            upload_file_to_s3(train_df_name, job_config.proc_data_bucket, train_key, job_config, remove_on_success=True)
                            test_key = make_s3_key_path(job_config, course, os.path.basename(test_df_name), session)
                            upload_file_to_s3(test_df_name, job_config.proc_data_bucket, test_key, job_config, remove_on_success=True)
                        except Exception as e:
                            logger.warning("exception occurrec while uploading cv results: {}".format(e))
        pool.close()
        pool.join()
    return


def create_session_folds(label_type, k = 5, multithread = True, raw_data_dir="morf-data/"):
    """
    From extract and extract-holdout data, create k randomized folds for each session and archive results to s3.
    :param label_type: type of outcome label to use.
    :param k: number of folds.
    :param multithread: logical indicating whether multiple cores should be used (if available)
    :param raw_data_dir: name of subfolder in s3 buckets containing raw data.
    :return:
    """
    user_id_col = "userID"
    label_col = "label_value"
    job_config = MorfJobConfig(CONFIG_FILENAME)
    mode = "cv"
    job_config.update_mode(mode)
    logger = set_logger_handlers(module_logger, job_config)
    # clear any preexisting data for this user/job/mode
    clear_s3_subdirectory(job_config)
    if multithread:
        num_cores = job_config.max_num_cores
    else:
        num_cores = 1
    logger.info("creating cross-validation folds")
    with Pool(num_cores) as pool:
        for raw_data_bucket in job_config.raw_data_buckets:
            for course in fetch_courses(job_config, raw_data_bucket):
                for session in fetch_sessions(job_config, raw_data_bucket, data_dir=raw_data_dir, course=course, fetch_all_sessions=True):
                    with tempfile.TemporaryDirectory(dir=job_config.local_working_directory) as working_dir:
                        input_dir, output_dir = initialize_input_output_dirs(working_dir)
                        # get the session feature and label data
                        download_train_test_data(job_config, raw_data_bucket, raw_data_dir, course, session, input_dir, label_type=label_type)
                        feature_file = os.path.join(input_dir, course, session, make_feature_csv_name(course, session))
                        label_file = os.path.join(input_dir, course, session, make_label_csv_name(course, session))
                        feat_df = pd.read_csv(feature_file, dtype=object)
                        label_df = pd.read_csv(label_file, dtype=object)
                        # merge features to ensure splits are correct
                        feat_label_df = pd.merge(feat_df, label_df, on=user_id_col)
                        assert feat_df.shape[0] == label_df.shape[0], "features and labels must contain same number of observations"
                        # create the folds
                        logger.info("creating cv splits with k = {} course {} session {}".format(k, course, session))
                        skf = StratifiedKFold(n_splits=k, shuffle=True)
                        folds = skf.split(np.zeros(feat_df.shape[0]), feat_label_df.label_value)
                        for fold_num, train_test_indices in enumerate(folds,1): # write each fold train/test data to csv and push to s3
                            train_index, test_index = train_test_indices
                            train_df,test_df = feat_label_df.loc[train_index,].drop(label_col, axis = 1), feat_label_df.loc[test_index,].drop(label_col, axis = 1)
                            train_df_name = os.path.join(working_dir, make_feature_csv_name(course, session, fold_num, "train"))
                            test_df_name = os.path.join(working_dir, make_feature_csv_name(course, session, fold_num, "test"))
                            train_df.to_csv(train_df_name, index = False)
                            test_df.to_csv(test_df_name, index=False)
                            # upload to s3
                            try:
                                train_key = make_s3_key_path(job_config, course, os.path.basename(train_df_name), session)
                                upload_file_to_s3(train_df_name, job_config.proc_data_bucket, train_key, job_config, remove_on_success=True)
                                test_key = make_s3_key_path(job_config, course, os.path.basename(test_df_name), session)
                                upload_file_to_s3(test_df_name, job_config.proc_data_bucket, test_key, job_config, remove_on_success=True)
                            except Exception as e:
                                logger.warning("exception occurrec while uploading cv results: {}".format(e))
        pool.close()
        pool.join()
    return


def cross_validate_session(label_type, k = 5, multithread = True, raw_data_dir="morf-data/"):
    """
    Compute k-fold cross-validation across sessions.
    :return:
    """
    # todo: call to create_session_folds() goes here
    job_config = MorfJobConfig(CONFIG_FILENAME)
    mode = "cv"
    job_config.update_mode(mode)
    logger = set_logger_handlers(module_logger, job_config)
    # clear any preexisting data for this user/job/mode
    # clear_s3_subdirectory(job_config)
    if multithread:
        num_cores = job_config.max_num_cores
    else:
        num_cores = 1
    logger.info("conducting cross validation")
    with Pool(num_cores) as pool:
        for raw_data_bucket in job_config.raw_data_buckets:
            for course in fetch_courses(job_config, raw_data_bucket):
                for session in fetch_sessions(job_config, raw_data_bucket, data_dir=raw_data_dir, course=course, fetch_all_sessions=True):
                    for fold_num in range(1, k+1):
                        with tempfile.TemporaryDirectory(dir=job_config.local_working_directory) as working_dir:
                            # get fold train data
                            input_dir, output_dir = initialize_input_output_dirs(working_dir)
                            session_input_dir = os.path.join(input_dir, course, session)
                            session_output_dir = os.path.join(output_dir, course, session)
                            trainkey = make_s3_key_path(job_config, course, make_feature_csv_name(course, session, fold_num, "train"), session)
                            train_data_path = download_from_s3(job_config.proc_data_bucket, trainkey, job_config.initialize_s3(), dir=session_input_dir)
                            testkey = make_s3_key_path(job_config, course, make_feature_csv_name(course, session, fold_num, "test"), session)
                            test_data_path = download_from_s3(job_config.proc_data_bucket, testkey, job_config.initialize_s3(), dir=session_input_dir)
                            # get labels
                            initialize_labels(job_config, raw_data_bucket, course, session, label_type, session_input_dir, raw_data_dir)
                            # run docker image with mode == cv
                            import ipdb;ipdb.set_trace()

                            # upload results
        pool.close()
        pool.join()
    return
