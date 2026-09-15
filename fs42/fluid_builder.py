import logging
import sqlite3
import sys
import os

sys.path.append(os.getcwd())

from fs42.fluid_statements import FluidStatements
from fs42.media_processor import MediaProcessor
from fs42.scheduling_context import (
    ValidationCatalogMetadataUnavailable,
    in_validation_mode,
)
from fs42.chapter_analysis import ChapterAnalysisError, analyze_chapters
from fs42.station_manager import StationManager

class FluidBuilder:
    def __init__(self, db_path=None):
        if db_path is None:
            self.db_path = StationManager().server_conf["db_path"]

        self._l = logging.getLogger("FLUID")
        connection = sqlite3.connect(self.db_path)
        try:
            FluidStatements.init_db(connection)
            connection.commit()
        finally:
            connection.close()

    def scan_file_cache(self, content_dir, media_filter="video"):
        connection = sqlite3.connect(self.db_path)
        try:
            # read all the files in the content dir
            self._l.info(f"Fluid file cache scan - reading {content_dir} with media_filter={media_filter}")
            file_list = MediaProcessor.rich_find_media(content_dir, media_filter)
            self._l.info(f"Comparing cache against {len(file_list)} files")
            # add any that aren't there yet
            FluidStatements.iterate_file_entries(connection, file_list)
            self._l.info("Checking file meta for stale entries.")
        finally:
            connection.close()

    def check_file_cache(self, full_path):
        connection = sqlite3.connect(self.db_path)
        try:
            results = FluidStatements.check_file_cache(connection, full_path)
        finally:
            connection.close()
        return results

    def trim_file_cache(self, from_time):
        connection = sqlite3.connect(self.db_path)
        try:
            self._l.info("Trimming fluid file cache")
            FluidStatements.trim_file_entries(connection, from_time)
        finally:
            connection.close()

    def scan_breaks(self, dir_path):
        connection = sqlite3.connect(self.db_path)
        try:
            self._l.info(f"Scanning directory {dir_path} for breaks")
            if not os.path.isdir(dir_path):
                raise FileNotFoundError(f"Directory does not exist {dir_path}")
            dir_path = os.path.realpath(dir_path)
            file_list = MediaProcessor._rfind_media(dir_path)

            # Check the cache because we require the duration to prococess.
            file_paths = [os.path.realpath(file) for file in file_list]
            cached_files = {}
            for path in file_paths:
                cached = FluidStatements.check_file_cache(connection, path)
                if cached:
                    cached_files[path] = cached

            for file in file_list:
                rfp = os.path.realpath(file)
                if rfp in cached_files:
                    cached = cached_files[rfp]
                    if FluidStatements.get_break_points(connection, rfp):
                        self._l.info(f"Breaks already exists for {rfp}")
                    else:
                        breaks = MediaProcessor.black_detect(rfp, cached.duration)
                        FluidStatements.add_break_points(connection, rfp, breaks)
                else:
                    self._l.warning(f"{rfp} is not in catalog cache - not adding break points.")
            connection.commit()
        finally:
            connection.close()

    def get_breaks(self, full_path):
        #fname = os.path.realpath(fname)
        connection = sqlite3.connect(self.db_path)
        try:
            results = FluidStatements.get_break_points(connection, full_path)
        finally:
            connection.close()
        return results

    def scan_chapters(self, dir_path):
        connection = sqlite3.connect(self.db_path)
        try:
            self._l.info(f"Scanning directory {dir_path} for chapters")
            if not os.path.isdir(dir_path):
                raise FileNotFoundError(f"Directory does not exist {dir_path}")
            dir_path = os.path.realpath(dir_path)
            file_list = MediaProcessor._rfind_media(dir_path)

            # Check the cache because we require the duration to process.
            file_paths = [os.path.realpath(file) for file in file_list]
            cached_files = {}
            for path in file_paths:
                cached = FluidStatements.check_file_cache(connection, path)
                if cached:
                    cached_files[path] = cached

            for file in file_list:
                rfp = os.path.realpath(file)
                if rfp in cached_files:
                    cached = cached_files[rfp]
                    try:
                        previous = FluidStatements.classify_chapter_points(
                            connection, rfp)
                    except ValueError:
                        self._l.error("Stored chapter metadata is invalid")
                        continue
                    if previous["status"] in {"trusted_v1", "legacy_nonempty"}:
                        self._l.info(f"Chapters already exist for {rfp}")
                    else:
                        if not FluidStatements._chapter_baseline_is_durable():
                            self._l.info(
                                "Chapter scan deferred until migration baseline is durable")
                            continue
                        try:
                            before = os.stat(rfp)
                            analysis = analyze_chapters(rfp, cached.duration)
                            info = os.stat(rfp)
                            if (before.st_dev, before.st_ino, before.st_size,
                                    before.st_mtime_ns) != (
                                    info.st_dev, info.st_ino, info.st_size,
                                    info.st_mtime_ns):
                                raise ChapterAnalysisError("chapter_data_invalid")
                            with connection:
                                FluidStatements.add_chapter_points(
                                    connection, rfp, analysis, info, previous)
                        except (ChapterAnalysisError, OSError, RuntimeError):
                            self._l.error("Chapter analysis failed")
                else:
                    self._l.warning(f"{rfp} is not in catalog cache - not adding chapter points.")
        finally:
            connection.close()

    def get_chapters(self, full_path):
        connection = sqlite3.connect(self.db_path)
        try:
            results = FluidStatements.get_chapter_points(connection, full_path)
        finally:
            connection.close()
        return results

    def scan_chapters_for_entries(self, entries):
        """Scan chapter markers for a list of catalog entries that don't have them yet"""
        connection = sqlite3.connect(self.db_path)
        try:
            for entry in entries:
                if hasattr(entry, 'realpath') and entry.realpath:
                    try:
                        previous = FluidStatements.classify_chapter_points(
                            connection, entry.realpath)
                    except ValueError as exc:
                        if in_validation_mode():
                            raise ValidationCatalogMetadataUnavailable(
                                "cached chapter metadata is invalid") from exc
                        self._l.error("Stored chapter metadata is invalid")
                        continue
                    if previous["status"] in {"trusted_v1", "legacy_nonempty"}:
                        continue
                    if in_validation_mode():
                        raise ValidationCatalogMetadataUnavailable(
                            "cached chapter metadata is missing")
                    if not FluidStatements._chapter_baseline_is_durable():
                        self._l.info(
                            "Chapter scan deferred until migration baseline is durable")
                        continue
                    try:
                        before = os.stat(entry.realpath)
                        analysis = analyze_chapters(entry.realpath, entry.duration)
                        info = os.stat(entry.realpath)
                        if (before.st_dev, before.st_ino, before.st_size,
                                before.st_mtime_ns) != (
                                info.st_dev, info.st_ino, info.st_size,
                                info.st_mtime_ns):
                            raise ChapterAnalysisError("chapter_data_invalid")
                        with connection:
                            FluidStatements.add_chapter_points(
                                connection, entry.realpath, analysis, info, previous)
                        if analysis.chapters:
                            self._l.info(
                                f"Added {len(analysis.chapters)} chapters for media")
                    except (ChapterAnalysisError, OSError, RuntimeError):
                        self._l.error("Chapter analysis failed")
        finally:
            connection.close()


if __name__ == "__main__":
    logging.basicConfig(format="%(levelname)s:%(name)s:%(message)s", level=logging.INFO)
    builder = FluidBuilder()
    # builder.scan_file_cache("catalog/nbc_content/")
    # exists = builder.check_file_cache("FieldStation42/catalog/public_domain/bextra/post-black.mov")
    builder.scan_breaks("catalog/public_domain/feature/sub/a/")
