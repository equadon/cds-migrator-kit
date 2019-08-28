# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS Migrator Records logging handler."""

import logging

from .log import JsonLogger

cli_logger = logging.getLogger('migrator')


def migration_exception_handler(logger):
    def inner(exc, output, key, value, **kwargs):
        """Migration exception handling - log to files.

        :param exc: exception
        :param output: generated output version
        :param key: MARC field ID
        :param value: MARC field value
        :return:
        """
        recid = output.get('recid', None)
        if not recid and 'legacy_recid' in output:
            recid = output['legacy_recid']
        else:
            recid = 'no recid found'
        cli_logger.error(
            '#RECID: #{0} - {1}  MARC FIELD: *{2}*, input value: {3}, -> {4}, '
            .format(recid, exc.message, key, value, output)
        )
        logger.add_log(exc, key, value, output, **kwargs)
    return inner
