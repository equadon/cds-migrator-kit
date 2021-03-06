# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS Migrator Records CLI."""

import json
import logging

import click
from cds_dojson.marc21.models.books.multipart import model as multipart_model
from cds_dojson.marc21.models.books.serial import model as serial_model
from flask import current_app
from flask.cli import with_appcontext

from .errors import LossyConversion
from .log import JsonLogger
from .records import CDSRecordDump

cli_logger = logging.getLogger(__name__)


def load_records(sources, source_type, eager, model=None, rectype=None):
    """Load records."""
    logger = JsonLogger.get_json_logger(rectype)

    for idx, source in enumerate(sources, 1):
        click.secho('Loading dump {0} of {1} ({2})'.format(
            idx, len(sources), source), fg='yellow')
        content = None
        with open(source.name, 'r+') as file:
            content = file.read().encode('UTF-8')
        with open(source.name, 'wb') as file:
            file.write(content)
            file.close()
        data = json.load(source)
        source.close()
        with click.progressbar(data) as records:
            for item in records:
                dump = CDSRecordDump(
                    data=item,
                    dojson_model=model,
                    logger=logger
                )
                click.echo('Processing item {0}...'.format(item['recid']))
                logger.add_recid_to_stats(item['recid'])
                try:
                    dump.prepare_revisions()
                    logger.add_record(dump.revisions[-1][1])

                except LossyConversion as e:
                    cli_logger.error('[DATA ERROR]: {0}'.format(e.message))
                    logger.add_log(e, output=item)
                # except AttributeError as e:
                #     current_app.logger.error('Model missing')
                #     JsonLogger().add_log(e, output=item, rectype=rectype)
                #     raise e
                # except TypeError as e:
                #     current_app.logger.error(
                #         'Model missing recid:{}'.format(item['recid']))
                #     JsonLogger().add_log(e, output=item, rectype=rectype)
                #     raise e
                # except KeyError as e:
                #     current_app.logger.error(
                #         'Model missing recid:{}'.format(item['recid']))
                #     JsonLogger().add_log(e, output=item, rectype=rectype)
                except Exception as e:
                    cli_logger.error(e)
                    current_app.logger.error(e)
                    logger.add_log(e, output=item)
                    raise e
        logger.save()
        click.secho('Check completed. See the report on: '
                    'books-migrator-dev.web.cern.ch/results', fg='green')


@click.group()
def report():
    """CDS Migrator report commands."""


@report.command()
@click.argument('sources', type=click.File('r', encoding='UTF-8',
                                           errors='replace'), nargs=-1)
@click.option(
    '--source-type',
    '-t',
    type=click.Choice(['json', 'marcxml']),
    default='marcxml',
    help='Whether to use JSON or MARCXML.')
@click.option(
    '--recid',
    '-r',
    help='Record ID to load (NOTE: will load only one record!).',
    default=None)
@click.option(
    '--rectype',
    '-x',
    help='Type of record to load (f.e serial).',
    default='document')
@with_appcontext
def dryrun(sources, source_type, recid, rectype, model=None):
    """Load records migration dump."""
    if rectype == 'multipart':
        model = multipart_model
    elif rectype == 'serial':
        model = serial_model
    load_records(sources=sources, source_type=source_type, eager=True,
                 model=model, rectype=rectype)
