import os

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
PROJ_ROOT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))
DATASET_DIR = os.path.abspath(os.path.join(PROJ_ROOT_DIR, 'datasets'))
OUTPUT_DIR = os.path.abspath(os.path.join(PROJ_ROOT_DIR, 'outputs'))

class XcalField:
    TIMESTAMP = 'TIME_STAMP'
    TECH = 'Event Technology'
    BAND = 'Event Technology(Band)'
    PCELL_FREQ_5G = '5G KPI PCell RF Frequency [MHz]'
    SMART_TPUT_DL = 'Smart Phone Smart Throughput Mobile Network DL Throughput [Mbps]'
    SMART_TPUT_UL = 'Smart Phone Smart Throughput Mobile Network UL Throughput [Mbps]'
    EVENT_LTE = 'Event LTE Events'
    EVENT_5G = 'Event 5G-NR Events'
    EVENT_5G_LTE = 'Event 5G-NR/LTE Events'
    LON = 'Lon'
    LAT = 'Lat'
    # Custom fields
    RUN_ID = 'run_id'
    SEGMENT_ID = 'segment_id'
    CUSTOM_UTC_TIME = 'utc_dt'
    LOCAL_TIME = 'local_dt'
    ACTUAL_TECH = 'actual_tech'
    TPUT_DL = 'tput_dl'
    TPUT_UL = 'tput_ul'
    APP_TPUT_PROTOCOL = 'app_tput_protocol'
    APP_TPUT_DIRECTION = 'app_tput_direction'

class XcalTech:
    LTE = 'LTE'
    NO_SERVICE = 'NO SERVICE'


class XcallHandoverEvent:
    HANDOVER_SUCCESS = 'Handover Success'
    NR_TO_EUTRA_REDIRECTION_SUCCESS = 'NR To EUTRA Redirection Success'
    NR_INTERFREQ_HANDOVER_SUCCESS = 'NR Interfreq Handover Success'
    UL_INFORMATION_TRANSFER_MRDC = 'ulInformationTransferMRDC'
    MCG_DRB_SUCCESS = 'MCG DRB Success'
    NR_SCG_ADDITION_SUCCESS = 'NR SCG Addition Success'
    MOBILITY_FROM_NR_TO_EUTRA_SUCCESS = 'Mobility from NR to EUTRA Success'
    NR_INTRA_FREQ_HANDOVER_SUCCESS = 'NR Intrafreq Handover Success'
    NR_SCG_MODIFICATION_SUCCESS = 'NR SCG Modification Success'
    SCG_FAILURE_INFORMATION_NR = 'scgFailureInformationNR'
    EUTRA_TO_NR_REDIRECTION_SUCCESS = 'EUTRA To NR Redirection Success'

    @classmethod
    def get_all_events(cls):
        return [
            cls.HANDOVER_SUCCESS,
            cls.NR_TO_EUTRA_REDIRECTION_SUCCESS,
            cls.NR_INTERFREQ_HANDOVER_SUCCESS,
            cls.UL_INFORMATION_TRANSFER_MRDC,
            cls.MCG_DRB_SUCCESS,
            cls.NR_SCG_ADDITION_SUCCESS,
            cls.MOBILITY_FROM_NR_TO_EUTRA_SUCCESS,
            cls.NR_INTRA_FREQ_HANDOVER_SUCCESS,
            cls.NR_SCG_MODIFICATION_SUCCESS,
            cls.SCG_FAILURE_INFORMATION_NR,
            cls.EUTRA_TO_NR_REDIRECTION_SUCCESS,
        ]