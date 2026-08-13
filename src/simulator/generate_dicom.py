"""
Synthetic DICOM file factory — no PHI, no real patient data.

Public API:
  make_ct_8x8()   -->     generate 8x8 DICOM image
  make_sized(kb)  -->     generate arbitrary kb size DICOM image
"""

import math
import os

from pydicom import FileDataset, FileMetaDataset
from pydicom import uid as dcm_uid

# Fixed sample ids, Orthanc deduplicates (0xB007) after the first upload.
_RTT_SOP_UID = "1.2.826.0.1.3680043.0.5.0.1"
_RTT_STUDY_UID = "1.2.826.0.1.3680043.0.5.0.2"
_RTT_SERIES_UID = "1.2.826.0.1.3680043.0.5.0.3"

# Shared PatientID across every synthetic instance
PATIENT_ID = "SIM001"


def _base_dataset(sop_uid: str, study_uid: str, series_uid: str) -> FileDataset:
    """Build a minimal but valid DICOM dataset."""

    file_meta = FileMetaDataset()
    file_meta.MediaStorageSOPClassUID = dcm_uid.SecondaryCaptureImageStorage
    file_meta.MediaStorageSOPInstanceUID = dcm_uid.UID(sop_uid)
    file_meta.TransferSyntaxUID = dcm_uid.ExplicitVRLittleEndian

    ds = FileDataset(
        filename_or_obj="",
        dataset={},
        file_meta=file_meta,
        preamble=b"\x00" * 128,
    )

    # Patient module
    ds.PatientName = "TEST^SIMULATOR"
    ds.PatientID = PATIENT_ID

    # Study / Series / Instance identity
    ds.StudyInstanceUID = study_uid
    ds.SeriesInstanceUID = series_uid
    ds.SOPInstanceUID = sop_uid
    ds.SOPClassUID = dcm_uid.SecondaryCaptureImageStorage

    # Pixel format tags
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.BitsAllocated = 8
    ds.BitsStored = 8
    ds.HighBit = 7
    ds.PixelRepresentation = 0

    return ds


def make_ct_8x8() -> FileDataset:
    """Return minimum fixed-UID 8×8 image."""
    ds = _base_dataset(_RTT_SOP_UID, _RTT_STUDY_UID, _RTT_SERIES_UID)
    side = 8
    ds.Rows = side
    ds.Columns = side
    ds.PixelData = bytes(side * side)
    return ds


def make_sized(kb: int, random_pixels: bool = False, study_uid: str | None = None) -> FileDataset:
    """Return a synthetic image of approximately kb kilobytes.

    Each call generates a fresh random SOP UID when distinct instances are needed.

    random_pixels: use os.urandom instead of all-zero bytes for the pixel data.
    study_uid: share one StudyInstanceUID across multiple calls; a fresh
    UID is generated per call when omitted.
    """
    sop_uid = dcm_uid.generate_uid()
    study_uid = study_uid or str(dcm_uid.generate_uid())
    series_uid = dcm_uid.generate_uid()

    ds = _base_dataset(sop_uid, study_uid, series_uid)
    side = math.ceil(math.sqrt(kb * 1024))
    n_bytes = side * side

    ds.Rows = side
    ds.Columns = side
    ds.PixelData = os.urandom(n_bytes) if random_pixels else bytes(n_bytes)
    return ds
