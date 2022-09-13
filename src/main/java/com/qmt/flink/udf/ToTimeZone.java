package com.qmt.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;

public class ToTimeZone extends ScalarFunction {
  private static final Map<String, String> MARKETPLACE_ID_TO_TIMEZONE = new HashMap<String, String>();
  static{
    MARKETPLACE_ID_TO_TIMEZONE.put('ATVPDKIKX0DER',  'America/Los_Angeles'),
    MARKETPLACE_ID_TO_TIMEZONE.put('A2EUQ1WTGCTBG2', 'America/Los_Angeles'),
    MARKETPLACE_ID_TO_TIMEZONE.put('A1AM78C64UM0Y8', 'America/Mexico_City'),
    MARKETPLACE_ID_TO_TIMEZONE.put('A2Q3Y263D00KWC', 'America/Sao_Paulo'),
    MARKETPLACE_ID_TO_TIMEZONE.put('A1F83G8C2ARO7P', 'Europe/London'),
    MARKETPLACE_ID_TO_TIMEZONE.put('A1PA6795UKMFR9', 'Europe/Berlin'),
    MARKETPLACE_ID_TO_TIMEZONE.put('A13V1IB3VIYZZH', 'Europe/Paris'),
    MARKETPLACE_ID_TO_TIMEZONE.put('A1RKKUPIHCS9HS', 'Europe/Madrid'),
    MARKETPLACE_ID_TO_TIMEZONE.put('APJ6JRA9NG5V4',  'Europe/Rome'),
    MARKETPLACE_ID_TO_TIMEZONE.put('A1805IZSGTT6HS', 'Europe/Amsterdam'),
    MARKETPLACE_ID_TO_TIMEZONE.put('A2NODRKZP88ZB9', 'Europe/Stockholm'),
    MARKETPLACE_ID_TO_TIMEZONE.put('A1C3SOZRARQ6R3', 'Europe/Warsaw'),
    MARKETPLACE_ID_TO_TIMEZONE.put('A33AVAJ2PDY3EV', 'Europe/Istanbul'),
    MARKETPLACE_ID_TO_TIMEZONE.put('A2VIGQ35RCS4UG', 'Asia/Dubai'),
    MARKETPLACE_ID_TO_TIMEZONE.put('A17E79C6D8DWNP', 'Asia/Riyadh'),
    MARKETPLACE_ID_TO_TIMEZONE.put('A21TJRUUN4KGV',  'Asia/Kolkata'),
    MARKETPLACE_ID_TO_TIMEZONE.put('A1VC38T7YXB528', 'Asia/Tokyo'),
    MARKETPLACE_ID_TO_TIMEZONE.put('A39IBJ37TRP1C6', 'Australia/Sydney'),
    MARKETPLACE_ID_TO_TIMEZONE.put('A19VAU5U5O7RUS', 'Asia/Singapore'),
  }
  public long eval(String marketplaceId) {
    return MARKETPLACE_ID_TO_TIMEZONE.get(marketplaceId)
  }
}
