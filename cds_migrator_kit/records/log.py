# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS Migrator Records loggers."""

import copy
import json
import logging
import os

from fuzzywuzzy import fuzz

from cds_dojson.marc21.fields.books.errors import ManualMigrationRequired, \
    MissingRequiredField, UnexpectedValue
from flask import current_app

from cds_migrator_kit.records.errors import LossyConversion


def set_logging():
    """Sets additional logging to file for debug."""
    logger_migrator = logging.getLogger('migrator')
    logger_migrator.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - '
                                  '%(message)s - \n '
                                  '[in %(pathname)s:%(lineno)d]')
    fh = logging.FileHandler('migrator.log')
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)
    logger_migrator.addHandler(fh)
    logger_matcher = logging.getLogger('cds_dojson.matcher.dojson_matcher')
    logger_matcher.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - '
                                  '%(message)s - \n '
                                  '[in %(pathname)s:%(lineno)d]')
    fh = logging.FileHandler('matcher.log')
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)
    logger_matcher.addHandler(fh)

    return logger_migrator


logger = logging.getLogger('migrator')


class JsonLogger(object):
    """Log migration statistic to file controller."""
    LOG_FILEPATH = None

    @classmethod
    def get_json_logger(cls, rectype):
        if rectype == 'serial':
            return SerialJsonLogger()
        else:
            return DocumentJsonLogger()

    def __init__(self, log_filename):
        """Constructor."""
        self._logs_path = current_app.config['CDS_MIGRATOR_KIT_LOGS_PATH']
        self.stats = {}
        self.records = {}
        self.STAT_FILEPATH = os.path.join(self._logs_path, log_filename)
        self.RECORD_FILEPATH = os.path.join(self._logs_path, 'records.json')

        if not os.path.exists(self._logs_path):
            os.makedirs(self._logs_path)

    def load(self):
        """Load stats from file as json."""
        logger.warning(self.STAT_FILEPATH)
        with open(self.STAT_FILEPATH, "r") as f:
            self.stats = json.load(f)
        with open(self.RECORD_FILEPATH, "r") as f:
            self.records = json.load(f)

    def save(self):
        """Save stats from file as json."""
        logger.warning(self.STAT_FILEPATH)
        with open(self.STAT_FILEPATH, "w") as f:
            json.dump(self.stats, f)
        with open(self.RECORD_FILEPATH, "w") as f:
            json.dump(self.records, f)


    def create_output_file(self, file, output):
        """Create json preview output file."""
        try:
            filename = os.path.join(
                current_app.config['CDS_MIGRATOR_KIT_LOGS_PATH'],
                "{0}/{1}.json".format(output['_migration']['record_type'],
                                      file))
            with open(filename, "w+") as f:
                json.dump(output, f, indent=2)
        except Exception as e:
            raise e

    def add_recid_to_stats(self, recid):
        pass

    def add_record(self, record):
        pass

    def add_log(self, exc, key=None, value=None, output=None):
        """Add exception log."""
        recid = output.get('recid', None)
        self.resolve_error_type(exc, recid, key, value)

    def resolve_error_type(self, exc, recid, key, value):
        """Check the type of exception and log to dict."""
        rec_stats = self.stats[recid]
        rec_stats['clean'] = False
        if isinstance(exc, ManualMigrationRequired):
            rec_stats['manual_migration'].append(key)
        elif isinstance(exc, UnexpectedValue):
            rec_stats['unexpected_value'].append((key, value))
        elif isinstance(exc, MissingRequiredField):
            rec_stats['missing_required_field'].append(key)
        elif isinstance(exc, LossyConversion):
            rec_stats['lost_data'] = list(exc.missing)
        elif isinstance(exc, KeyError):
            rec_stats['unexpected_value'].append(str(exc))
        elif isinstance(exc, TypeError) or isinstance(exc, AttributeError):
            rec_stats['unexpected_value'].append(
                "Model definition missing for this record."
                " Contact CDS team to tune the query")
        else:
            raise exc


class DocumentJsonLogger(JsonLogger):
    """Log migration statistic to file controller."""

    def __init__(self):
        super().__init__('stats_document.json')

    def add_recid_to_stats(self, recid):
        """Add empty log item."""
        if recid not in self.stats:
            record_stats = {'recid': recid,
                            'manual_migration': [],
                            'unexpected_value': [],
                            'missing_required_field': [],
                            'lost_data': [],
                            'clean': True,
                            }
            self.stats[recid] = record_stats

    def add_record(self, record):
        self.records[record['recid']] = record

def same_issn(obj1, obj2):
    return obj1['issn'] is not None and obj2['issn'] is not None and obj1['issn'] == obj2['issn']

def compare_titles(title1, title2):
    return fuzz.ratio(title1, title2)

class SerialJsonLogger(JsonLogger):
    """Log migration statistic to file controller."""

    def __init__(self):
        """Constructor."""
        super().__init__('stats_serial.json')

    def add_log(self, exc, key=None, value=None, output=None):
        pass

    def add_recid_to_stats(self, recid):
        """Add empty log item."""
        pass

    def _add_to_stats(self, record):
        title = record['title']['title']
        if title in self.stats:
            self.stats[title]['documents'].append(record['recid'])
        else:
            self.stats[title] = {
                'title': title,
                'issn': record.get('issn', None),
                'documents': [record['recid']],
                'similars': {
                    'same_issn': [],
                    'similar_title': [],
                }
            }

    def _add_to_record(self, record):
        del record['recid']
        title = record['title']['title']
        self.records[title] = record

    def add_record(self, record):
        title = record['title']
        if len(title) > 1:
            for title in record['title']:
                new_record = copy.deepcopy(record)
                new_record['title'] = title
                self._add_to_stats(new_record)
                self._add_to_record(new_record)
        else:
            record['title'] = record['title'][0]
            self._add_to_stats(record)
            self._add_to_record(record)

    def resolve_error_type(self, exc, rec_stats, key, value):
        """Check the type of exception and log to dict."""
        rec_stats['clean'] = False
        if isinstance(exc, ManualMigrationRequired):
            rec_stats['manual_migration'].append(key)
        elif isinstance(exc, UnexpectedValue):
            rec_stats['unexpected_value'].append((key, value))
        elif isinstance(exc, MissingRequiredField):
            rec_stats['missing_required_field'].append(key)
        elif isinstance(exc, LossyConversion):
            rec_stats['lost_data'] = list(exc.missing)
        elif isinstance(exc, KeyError):
            rec_stats['unexpected_value'].append(str(exc))
        elif isinstance(exc, TypeError) or isinstance(exc, AttributeError):
            rec_stats['unexpected_value'].append(
                "Model definition missing for this record."
                " Contact CDS team to tune the query")
        else:
            raise exc

    def _add_children(self):
        for record in self.records.values():
            record['_migration']['children'] = self.stats[record['title']['title']]['documents']

    def _match_similar(self):
        items = self.stats.items()
        for title1, stat1_obj in items:
            for title2, stat2_obj in items:
                if title1 == title2:
                    continue
                if same_issn(stat1_obj, stat2_obj):
                    import ipdb; ipdb.set_trace()
                    stat1_obj['similars']['same_issn'].append(title2)
                    stat2_obj['similars']['same_issn'].append(title1)
                else:
                    ratio = compare_titles(title1, title2)
                    if 95 <= ratio < 100:
                        import ipdb; ipdb.set_trace()
                        stat1_obj['similars']['similar_title'].append(title2)
                        stat2_obj['similars']['similar_title'].append(title1)

    def save(self):
        self._add_children()
        self._match_similar()
        super().save()


# class RecordJsonLogger(object):
#     """Log migration statistic to file controller."""

#     def __init__(self):
#         """Constructor."""
#         self._logs_path = current_app.config['CDS_MIGRATOR_KIT_LOGS_PATH']

#         self.STAT_FILEPATH = os.path.join(self._logs_path, 'stats.json')
#         self.LOG_SERIALS = os.path.join(self._logs_path, 'serials.json')
#         if not os.path.exists(self._logs_path):
#             os.makedirs(self._logs_path)
#         if not os.path.exists(self.STAT_FILEPATH):
#             with open(self.STAT_FILEPATH, "w+") as f:
#                 json.dump([], f, indent=2)
#         if not os.path.exists(self.LOG_SERIALS):
#             with open(self.LOG_SERIALS, "w") as f:
#                 json.dump([], f, indent=2)

#     @staticmethod
#     def clean_stats_file():
#         """Removes contents of the statistics file."""
#         filepath = os.path.join(
#             current_app.config['CDS_MIGRATOR_KIT_LOGS_PATH'], 'stats.json')
#         with open(filepath, 'w') as f:
#             f.write('[]')
#             f.close()

#     @staticmethod
#     def get_stat_by_recid(recid, stats_json):
#         """Search for existing stats of given recid."""
#         return next(
#             (item for item in stats_json if item['recid'] == recid), None)

#     def render_stats(self):
#         """Load stats from file as json."""
#         logger.warning(self.STAT_FILEPATH)
#         with open(self.STAT_FILEPATH, "r") as f:
#             all_stats = json.load(f)
#             return all_stats

#     def create_output_file(self, file, output):
#         """Create json preview output file."""
#         try:
#             filename = os.path.join(
#                 current_app.config['CDS_MIGRATOR_KIT_LOGS_PATH'],
#                 "{0}/{1}.json".format(output['_migration']['record_type'],
#                                       file))
#             with open(filename, "w+") as f:
#                 json.dump(output, f, indent=2)
#         except Exception as e:
#             raise e

#     def add_recid_to_serial(self,  current_entry, similar_series, ratio):
#         """Add record id to existing serial stats."""
#         all_stats = JsonLogger().render_stats()
#         with open(self.STAT_FILEPATH, "w+") as f:
#             record_stats = JsonLogger.get_stat_by_recid(
#                 similar_series['recid'], all_stats)
#             if ratio < 100:
#                 record_stats['similar_series'].append(current_entry['recid'])
#             else:
#                 record_stats['exact_series'].append(current_entry['recid'])
#             json.dump(all_stats, f, indent=2)

#     def add_extracted_records(self, recid, index):
#         """Add additionally extracted records from many series."""
#         all_stats = JsonLogger().render_stats()
#         with open(self.STAT_FILEPATH, "w+") as f:
#             record_stats = JsonLogger.get_stat_by_recid(
#                 recid, all_stats)
#             record_stats['extracted_records'].append(index)
#             json.dump(all_stats, f, indent=2)

#     def add_log(self, exc, key=None, value=None, output=None, rectype=None):
#         """Add exception log."""
#         all_stats = JsonLogger().render_stats()
#         recid = output.get('recid', None)
#         if not recid:
#             recid = output.get('legacy_recid', None)
#         with open(self.STAT_FILEPATH, "w+") as f:
#             record_stats = JsonLogger.get_stat_by_recid(recid, all_stats)
#             if not record_stats:
#                 record_stats = {'recid': recid,
#                                 'record_type': rectype,
#                                 'manual_migration': [],
#                                 'unexpected_value': [],
#                                 'missing_required_field': [],
#                                 'lost_data': [],
#                                 'clean': False,
#                                 'similar_series': [],
#                                 'exact_series': [],
#                                 'extracted_records': []
#                                 }
#                 all_stats.append(record_stats)
#             self.resolve_error_type(exc, record_stats, key, value)
#             json.dump(all_stats, f, indent=2)

#     def add_recid_to_stats(self, output, rectype=None):
#         """Add empty log item."""
#         all_stats = JsonLogger().render_stats()
#         with open(self.STAT_FILEPATH, "w+") as f:
#             record_stats = JsonLogger.get_stat_by_recid(output['recid'],
#                                                         all_stats)
#             if not record_stats:
#                 record_stats = {'recid': output['recid'],
#                                 'record_type': rectype,
#                                 'manual_migration': [],
#                                 'unexpected_value': [],
#                                 'missing_required_field': [],
#                                 'lost_data': [],
#                                 'clean': True,
#                                 'similar_series': [],
#                                 'exact_series': [],
#                                 'extracted_records': [],
#                                 }
#                 all_stats.append(record_stats)
#                 json.dump(all_stats, f, indent=2)

#     def resolve_error_type(self, exc, rec_stats, key, value):
#         """Check the type of exception and log to dict."""
#         rec_stats['clean'] = False
#         if isinstance(exc, ManualMigrationRequired):
#             rec_stats['manual_migration'].append(key)
#         elif isinstance(exc, UnexpectedValue):
#             rec_stats['unexpected_value'].append((key, value))
#         elif isinstance(exc, MissingRequiredField):
#             rec_stats['missing_required_field'].append(key)
#         elif isinstance(exc, LossyConversion):
#             rec_stats['lost_data'] = list(exc.missing)
#         elif isinstance(exc, KeyError):
#             rec_stats['unexpected_value'].append(str(exc))
#         elif isinstance(exc, TypeError) or isinstance(exc, AttributeError):
#             rec_stats['unexpected_value'].append(
#                 "Model definition missing for this record."
#                 " Contact CDS team to tune the query")
#         else:
#             raise exc

#     def add_related_child(self, stored_parent, rectype, related_recid):
#         """Dumps recids picked up during migration in the output file."""
#         if '_index' in stored_parent:
#             filename = '{0}/{1}_{2}_{3}.json'.format(
#                 rectype, rectype, stored_parent['recid'],
#                 stored_parent['_index'])
#         else:
#             filename = '{0}/{1}_{2}.json'.format(rectype, rectype,
#                                                  stored_parent['recid'])
#         filepath = os.path.join(self._logs_path, filename)
#         with open(filepath, 'r+') as file:
#             parent = json.load(file)
#             key_name = '_migration_relation_{0}_recids'.format(rectype)
#             if key_name not in parent:
#                 parent[key_name] = []
#             parent[key_name].append(related_recid)
#             file.seek(0)
#             file.truncate(0)
#             json.dump(parent, file, indent=2)
