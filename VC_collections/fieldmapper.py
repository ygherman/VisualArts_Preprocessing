"""
SYNOPSIS
    TODO helloworld [-h,--help] [-v,--verbose] [--version]

DESCRIPTION
    TODO This describes how to use this script. This docstring
    will be printed by the script if there is an error or
    if the user requests help (-h or --help).
    
PROJECT NAME:
    helper_fuctions

AUTHOR
    Yael Vardina Gherman <Yael.VardinaGherman@nli.org.il>
    Yael Vardina Gherman <gh.gherman@gmail.com>

LICENSE
    This script is in the public domain, free from copyrights or restrictions.

VERSION
    Date: 22/08/2019 15:54
    
    $
"""

catalog_field_mapper = {
    "אוסףפתוח": "ACCURALS",
    "ארכיוןפתוח": "ACCURALS",
    "ביבליוגרפיהומקורותמידע": "BIBLIOGRAPHY",
    "ברקוד": "BARCODE",
    "בעליםנוכחי": "CURRENT_OWNER",
    "דיגיטציה": "DIGITIZATION",
    "היסטוריהארכיונית": "BIOGHIST",
    "היקף": "EXTENT",
    "היקףהחומר": "EXTENT",
    "הערות": "NOTES",
    "הערותגלויתלמשתמשקצה": "NOTES",
    "הערותגלוילמשתמשקצה": "NOTES",
    "הערותגלויתלמשתמש": "NOTES",
    "הערותלאגלוילמשתמשקצה": "NOTES_HIDDEN",
    "הערותחסויותלמשתמש": "NOTES_HIDDEN",
    "הערותלאגלוילמשתמש": "NOTES_HIDDEN",
    "הערותגלויותלמשתמש": "NOTES",
    "חומריםקשורים": "RELATED_MATERIALS",
    "טכניקה": "TECHNIQUE",
    "יוצרים": "COMBINED_CREATORS",
    "יוצריהאוסף": "COLLECTION_CREATOR",
    "יוצריהארכיון": "COLLECTION_CREATOR",
    "יוצריםנוספים": "ADD_CREATORS",
    "יוצריםנוספיםאיש": "ADD_CREATOR_PERS",
    "שמותתאגידיםיוצריםנוספים": "ADD_CREATOR_CORPS",
    "יוצריםנוספיםמוסד": "ADD_CREATOR_CORPS",
    "יוצרראשיאיש": "FIRST_CREATOR_PERS",
    "יוצרראשימוסד": "FIRST_CREATOR_CORP",
    "כותר": "UNITITLE",
    "כותרת": "UNITITLE",
    "כותרתאנגלית": "UNITITLE_ENG",
    "כותרתערבית": "UNITITLE_AR",
    "כותרתמשנה": "SECONDARY_UNITITLE",
    "כותרתמתורגמת": "TRANSLATED_UNITITLE",
    "למחיקה": "TO_DELETE",
    "מגבלותלתצוגהבאינטרנט": "ACCESSRESTRICT",
    "מגבלותפרטיות": "ACCESSRESTRICT",
    "מדיהפורמט": "MEDIUM_FORMAT",
    "מדינתהפרסום": "PUBLICATION_COUNTRY",
    "מדינתפרסום": "PUBLICATION_COUNTRY",
    "מדינתהפרסוםהצילום": "PUBLICATION_COUNTRY",
    "מידות": "DIMENSIONS",
    "מידענוסף": "SCOPECONTENT",
    "מידעעלהצטברותהאוסף": "BIOGHIST",
    "מידעעלהצטברותהחומר": "BIOGHIST",
    "מידעעלסידורהאוסףשיטתהסידור": "ARRANGEMENT",
    "שיטתהסידור": "ARRANGEMENT",
    "מידעעלסידורהחומר": "ARRANGEMENT",
    "מיכל": "CONTAINER",
    "מילותמפתחאישיליבה": "PERSNAME",
    "מילותמפתחיצירותליבה": "WORKS",
    "מילותמפתחמוסדותליבה": "CORPNAME",
    "מילותמפתחנושאיליבה": "SUBJECT",
    "מילותמפתחאישים": "PERSNAME",
    "מילותמפתחארגונים": "CORPNAME",
    "מילותמפתחיצירות": "WORKS",
    "מילותמפתחמוסדות": "CORPNAME",
    "מילותמפתחמקומות": "GEOGNAME",
    "מילותמפתחנושאים": "SUBJECT",
    "מיקוםפיזי": "PHYSLOC",
    "מסלולדיגיטציה": "DIGITIZATION",
    "מספרהמיכל": "CONTAINER",
    "מספרהמיכלבונמצאהתיקפריט": "CONTAINER",
    "מספרמיכל": "CONTAINER",
    "מספרמערכתאלף": "MMS ID",
    "מספרקבציםלסריקה": "EST_FILES_NUM",
    "מספרקבציםמוערך": "EST_FILES_NUM",
    "מסקבציםמוערך": "EST_FILES_NUM",
    "מקוםהפרסום": "PUBLICATION_COUNTRY",
    "משך": "DURATION",
    "נשלחלדיגיטציה": "DIGITIZATION",
    "סוגאוסף": "COLLECTION_TYPE",
    "סוגארכיון": "ARCHIVE_TYPE",
    "סוגהחומר": "ARCHIVAL_MATERIAL",
    "סוגחומר": "ARCHIVAL_MATERIAL",
    "סוגיוצרראשיאיש": "TYPE_FIRST_CREATOR_PERS",
    "סוגיוצראיש": "TYPE_FIRST_CREATOR_PERS",
    "סוגיוצרראשימוסד": "TYPE_FIRST_CREATOR_CORP",
    "סוגיוצרמוסד": "TYPE_FIRST_CREATOR_CORP",
    "סימול": "UNITID",
    "סימולאב": "ROOTID",
    "סימולהאוסף": "UNITID",
    "סימולמספרמזהה": "UNITID",
    "סימולמקורי": "ORIGINAL_ID",
    "סימולפרויקט": "UNITID",
    "סריקהדוצדדית": "TWO_SIDE_SCAN",
    "סריקתדוצדדית": "TWO_SIDE_SCAN",
    "פומבי": "PUBLIC",
    "קודתיקארכיון": "ARCHIV_ID",
    "מספרתיקארכיון": "ARCHIV_ID",
    "קישורלסריקה": "IE",
    "קישוריםלסריקה": "IE",
    "קנהמידה": "SCALE",
    "רושם": "CATALOGUER",
    "רמתתיאור": "LEVEL",
    "שם": "UNITITLE",
    "שםהאוסף": "UNITITLE",
    "שםהמקטלג": "CATALOGUER",
    "שםהרושם": "CATALOGUER",
    "שםרושם": "CATALOGUER",
    "שםיוצרתאגיד": "FIRST_CREATOR_CORP",
    "שםיוצרראשימוסד": "FIRST_CREATOR_CORP",
    "שםיוצרמוסד": "FIRST_CREATOR_CORP",
    "שםיוצראיש": "FIRST_CREATOR_PERS",
    "שםיוצרראשיאיש": "FIRST_CREATOR_PERS",
    "שפה": "LANGUAGE",
    "תאריך": "DATE_NORMAL",
    "תאריךהרישום": "DATE_CATALOGING",
    "תאריךחופשי": "DATE",
    "תאריךמנורמל": "DATE_NORMAL",
    "תאריךמנורמלמאוחר": "DATE_END",
    "תאריךמנורמלמוקדם": "DATE_START",
    "תאריךפתיחתרשומה": "RECORD_CREATE_DATE",
    "תאריךקיטלוג": "DATE_CATALOGING",
    "תאריךרישום": "DATE_CATALOGING",
    "תאריךצילוםמנורמלמוקדם": "PHOTO_DATE_EARLY",
    "תאריךצילוםמנורמלמאוחר": "PHOTO_DATE_LATE",
    "תאריךתצלוםחפץטקסטמוערמוקדם": "PHOTO_DATE_EARLY",
    "תאריךיצירתהחפץמוקדם": "PHOTO_DATE_EARLY",
    "תאריךיצירתהחפץהטקסטהמקורימוקדם": "PHOTO_DATE_EARLY",
    "תאריךיצירתהחפץהטקסטהמקורימאוחר": "PHOTO_DATE_LATE",
    "תאריךיצירתהחפץמאוחר": "PHOTO_DATE_LATE",
    "תאריךתצלוםחפץטקסטמוערמאוחר": "PHOTO_DATE_LATE",
    "תיאור": "SCOPECONTENT",
    "תיאורהחומרבפרויקטתרבותחזותיתואמנויותהבמה": "APPRAISAL",
    "תיאורהטיפולבאוסףבפרויקט": "APPRAISAL",
    "הערותפנימיותלטיובסריקות": "NOTES_SCAN_LINK_REPAIR",
    "הערותפנימיותלטיוב": "NOTES_SCAN_LINK_REPAIR",
    "סימוללטיוב": "CALL_NUM_TO_LINK_REPAIR",
    "לסימול": "CALL_NUM_TO_LINK_REPAIR",
    "IE_to_delete": "IE_TO_DELETE",
    "IEtodelete": "IE_TO_DELETE",
    "למחיקהIE": "IE_TO_DELETE",
    "IE_to_export": "IE_TO_EXPORT",
    "IEtoexport": "IE_TO_EXPORT",
    "IEלייצוא": "IE_TO_EXPORT",
    'פריט ייעודי לצרכי תוכן ויח"צ': 'פריט ייעודי לצרכי תוכן ויח"צ',
    "פריטייעודילצרכיתוכןויחצ": 'פריט ייעודי לצרכי תוכן ויח"צ',
    "תאריךפקיעתהחיסיון": "תאריך פקיעת החיסיון",
}

field_mapper_back = {
    "ACCURALS": "אוסף פתוח",
    "BIBLIOGRAPHY": "ביבליוגרפיה ומקורות מידע",
    "BARCODE": "ברקוד",
    "BIOGHIST": "היסטוריה ארכיונית",
    "EXTENT": "היקף",
    "NOTES": "הערות גלוי למשתמש קצה",
    "NOTES_HIDDEN": "הערות לא גלוי למשתמש",
    "RELATED_MATERIALS": "חומרים קשורים",
    "TECHNIQUE": "טכניקה",
    "FIRST_CREATOR_PERS": "יוצר ראשי-איש",
    "FIRST_CREATOR_CORP": "יוצר ראשי-מוסד",
    "COMBINED_CREATORS": "יוצרים",
    "ADD_CREATOR_PERS": "יוצרים נוספים איש",
    "ADD_CREATOR_CORPS": "יוצרים נוספים מוסד",
    "UNITITLE": "כותרת",
    "UNITITLE_ENG": "כותרת אנגלית",
    "UNITITLE_AR": "כותרת ערבית",
    "TO_DELETE": "למחיקה",
    "ACCESSRESTRICT": "מגבלות פרטיות",
    "MEDIUM_FORMAT": "מדיה+פורמט",
    "PUBLICATION_COUNTRY": "מדינת הפרסום/הצילום",
    "DIMENSIONS": "מידות",
    "ARRANGEMENT": "מידע על סידור החומר",
    "CONTAINER": "מיכל",
    "PERSNAME": "מילות מפתח_אישים",
    "WORKS": "מילות מפתח_יצירות",
    "CORPNAME": "מילות מפתח_מוסדות",
    "GEOGNAME": "מילות מפתח_מקומות",
    "SUBJECT": "מילות מפתח_נושאים",
    "PHYSLOC": "מיקום פיזי",
    "DIGITIZATION": "מסלול דיגיטציה",
    "EST_FILES_NUM": "מספר קבצים מוערך",
    "COLLECTION_TYPE": "סוג אוסף",
    "ARCHIVAL_MATERIAL": "סוג חומר",
    "TYPE_FIRST_CREATOR_PERS": "סוג יוצר ראשי-איש",
    "TYPE_FIRST_CREATOR_CORP": "סוג יוצר ראשי-מוסד",
    "UNITID": "סימול",
    "ROOTID": "סימול אב",
    "ORIGINAL_ID": "סימול מקורי",
    "TWO_SIDE_SCAN": "סריקה דו צדדית",
    "PUBLIC": "פומבי",
    "ARCHIV_ID": "קוד תיק ארכיון",
    "SCALE": "קנה מידה",
    "LEVEL": "רמת תיאור",
    "CATALOGUER": "שם הרושם",
    "LANGUAGE": "שפה",
    "DATE": "תאריך חופשי",
    "DATE_NORMAL": "תאריך מנורמל",
    "DATE_END": "תאריך מנורמל מאוחר",
    "DATE_START": "תאריך מנורמל מוקדם",
    "RECORD_CREATE_DATE": "תאריך פתיחת רשומה",
    "DATE_CATALOGING": "תאריך רישום",
    "PHOTO_DATE_LATE": "תאריך תצלום  חפץ/טקסט מוער מאוחר",
    "PHOTO_DATE_EARLY": "תאריך תצלום חפץ/טקסט מוער מוקדם",
    "SCOPECONTENT": "תיאור",
    "APPRAISAL": "תיאורהטיפולבאוסףבפרויקט",
    "NOTES_SCAN_LINK_REPAIR": "הערות פנימיות לטיוב סריקות",
    "CALL_NUM_TO_LINK_REPAIR": "סימול לטיוב",
    "MMS ID": "מספר מערכת אלף",
    "SECONDARY_UNITITLE": "כותרת משנה",
    "TRANSLATED_UNITITLE": "כותרת מתורגמת",
}

level_mapper = {
    "אוסף": "Fonds Record",
    "חטיבה": "Sub-Fonds Record",
    "תתחטיבה": "Sub-Fonds Record",
    "תת-חטיבה": "Sub-Fonds Record",
    "תת חטיבה": "Sub-Fonds Record",
    "סדרה": "Series Record",
    "תתסדרה": "Sub-Series Record",
    "תת-סדרה": "Sub-Series Record",
    "תת סדרה": "Sub-Series Record",
    "תיק": "File Record",
    "פריט": "Item Record",
    "סידרה": "Series Record",
    "תתסידרה": "Sub-Series Record",
}

collection_field_mapper = {
    "סימול האוסף": "UNITID",
    "סימול הארכיון": "UNITID",
    "סימולהארכיון": "UNITID",
    "סימול": "UNITID",
    "סימולהאוסף": "UNITID",
    "סימול מקורי": "ORIGINAL_ID",
    "סימולמקורי": "ORIGINAL_ID",
    "רמת תיאור": "LEVEL",
    "רמתתיאור": "LEVEL",
    "שם האוסף": "UNITITLE",
    "שם הארכיון": "UNITITLE",
    "שםהארכיון": "UNITITLE",
    "שםהאוסף": "UNITITLE",
    "שםהאוסףבאנגלית": "UNITITLE_ENG",
    "שםהארכיוןבאנגלית": "UNITITLE_ENG",
    "כותרתערבית": "UNITITLE_AR",
    "כותרת": "UNITITLE",
    "תאריך חופשי": "DATE",
    "תאריךחופשי": "DATE",
    "יוצרי האוסף": "COLLECTION_CREATOR",
    "יוצריהאוסף": "COLLECTION_CREATOR",
    "יוצרי הארכיון": "COLLECTION_CREATOR",
    "יוצריהארכיון": "COLLECTION_CREATOR",
    "יוצרים": "COLLECTION_CREATOR",
    "תאריכים": "DATE",
    "מילותמפתחאישיליבה": "PERSNAME",
    "מילות מפתח_אישי ליבה": "PERSNAME",
    "מילות מפתח_מוסדות ליבה": "CORPNAME",
    "מילותמפתחמוסדותליבה": "CORPNAME",
    "מילות מפתח_יצירות ליבה": "WORKS",
    "מילותמפתחיצירותליבה": "WORKS",
    "מילות מפתח_נושאי ליבה": "SUBJECT",
    "מילותמפתחנושאיליבה": "SUBJECT",
    "סוג חומר": "ARCHIVAL_MATERIAL",
    "סוגחומר": "ARCHIVAL_MATERIAL",
    "היסטוריהארכיונית": "BIOGHIST",
    "היסטוריה ארכיונית": "BIOGHIST",
    "שיטת הסידור": "ARRANGEMENT",
    "שיטתהסידור": "ARRANGEMENT",
    "תיאורהטיפולבאוסףבפרויקט": "APPRAISAL",
    "תיאורהטיפולבארכיוןבפרויקט": "APPRAISAL",
    "תיאורהאוסףבפרויקטתרבותחזותיתואמנויותהבמה": "APPRAISAL",
    "תיאורהארכיוןבפרויקטתרבותחזותיתואמנויותהבמה": "APPRAISAL",
    "תיאור האוסף בפרויקט תרבות חזותית ואמנויות הבמה": "APPRAISAL",
    "תיאור הארכיון בפרויקט תרבות חזותית ואמנויות הבמה": "APPRAISAL",
    "סוג אוסף": "COLLECTION_TYPE",
    "סוג ארכיון": "COLLECTION_TYPE",
    "סוגארכיון": "COLLECTION_TYPE",
    "סוגאוסף": "COLLECTION_TYPE",
    "היקף": "EXTENT",
    "היקף החומר הפיזי טרום מיון": "EXTENT",
    "היקףהחומרהפיזיטרוםמיון": "EXTENT",
    "אוסף פתוח": "ACCURALS",
    "ארכיון פתוח": "ACCURALS",
    "אוסףפתוח": "ACCURALS",
    "ארכיוןפתוח": "ACCURALS",
    "ביבליוגרפיה ומקורות מידע": "BIBLIOGRAPHY",
    "ביבליוגרפיהומקורותמידע": "BIBLIOGRAPHY",
    "ביביליוגרפיהומקורותמידע": "BIBLIOGRAPHY",
    "ביביליוגרפיה ומקורות מידע": "BIBLIOGRAPHY",
    "בעלים נוכחי": "CURRENT_OWNER",
    "בעליםנוכחי": "CURRENT_OWNER",
    "מיקום פיזי": "PHYSLOC",
    "מיקוםפיזי": "PHYSLOC",
    "חומרים קשורים": "RELATED_MATERIALS",
    "חומריםקשורים": "RELATED_MATERIALS",
    "הערות - גלוי למשתמש קצה": "NOTES",
    "הערותגלוי למשתמש קצה": "NOTES",
    "הערותגלוילמשתמשקצה": "NOTES",
    "הערותלאגלוילמשתמשקצה": "NOTES_HIDDEN",
    "הערות - לא גלוי למשתמש קצה": "NOTES_HIDDEN",
    "שם הרושם": "CATALOGUER",
    "שםהרושם": "CATALOGUER",
    "תאריךהרישום": "DATE_CATALOGING",
    "תאריך הרישום": "DATE_CATALOGING",
    "הערותסוקר": "NOTES",
}

final_fields_back_mapper = {
    "ACCESSRESTRICT": "מגבלות פרטיות",
    "ACCURALS": "אוסף פתוח",
    "ADD_CREATOR_CORPS": "יוצרים נוספים - מוסד",
    "ADD_CREATOR_PERS": "יוצרים נוספים - איש",
    "APPRAISAL": "תיאור הטיפול באוסף בפרויקט",
    "ARCHIV_ID": "קוד תיק ארכיון",
    "ARCHIVAL_MATERIAL": "סוג חומר",
    "ARRANGEMENT": "מידע על סידור החומר",
    "BARCODE": "ברקוד",
    "BIBLIOGRAPHY": "ביבליוגרפיה ומקורות מידע",
    "BIOGHIST": "היסטוריה ארכיונית",
    "CATALOGUER": "שם הרושם",
    "COLLECTION_TYPE": "סוג אוסף",
    "COMBINED_CREATORS": "יוצרים",
    "COLLECTION_CREATOR": "יוצרי האוסף",
    "COMBINED_CREATORS_PERS": "יוצרים אישים",
    "COMBINED_CREATORS_CORPS": "יוצרים מוסדות",
    "CONTAINER": "מספר מיכל",
    "CORPNAME": "מילות מפתח - מוסדות",
    "CURRENT_OWNER": "בעלים נוכחי",
    "DATE": "תאריך חופשי",
    "DATE_CATALOGING": "תאריך הרישום",
    "DATE_END": "תאריך מנורמל מאוחר",
    "DATE_START": "תאריך מנורמל מוקדם",
    "DIGITIZATION": "מסלול דיגיטציה",
    "DURATION": "משך",
    "EST_FILES_NUM": "מספר קבצים מוערך",
    "EXTENT": "היקף החומר",
    "GEOGNAME": "מילות מפתח_מקומות",
    "FIRST_CREATOR_CORP": "יוצר ראשי - מוסד",
    "FIRST_CREATOR_PERS": "יוצר ראשי - איש",
    "LANGUAGE": "שפה",
    "LEVEL": "רמת תיאור",
    "MEDIUM_FORMAT": "מדיה + פורמט",
    "NOTES": "הערות גלוי למשתמש קצה",
    "NOTES_HIDDEN": "הערות לא גלוי למשתמש",
    "ORIGINAL_ID": "סימול מקורי",
    "PERSNAME": "מילות מפתח - אישים",
    "PHOTO_DATE_EARLY": "תאריך יצירת החפץ / הטקסט המקורי מוקדם",
    "PHOTO_DATE_LATE": "תאריך יצירת החפץ / הטקסט המקורי מאוחר",
    "PHYSLOC": "מיקום פיזי",
    "PUBLICATION_COUNTRY": "מדינת הפרסום/הצילום",
    "RELATED_MATERIALS": "חומרים קשורים",
    "ROOTID": "סימול אב",
    "SCALE": "קנה מידה",
    "SCOPECONTENT": "תיאור",
    "SUBJECT": "מילות מפתח_נושאים",
    "TECHNIQUE": "טכניקה",
    "TWO_SIDE_SCAN": "סריקה דו-צדדית",
    "TYPE_FIRST_CREATOR_CORP": "סוג יוצר ראשי - מוסד",
    "TYPE_FIRST_CREATOR_PERS": "סוג יוצר ראשי - איש",
    "UNITID": "סימול פרויקט",
    "UNITITLE": "כותרת",
    "UNITITLE_ENG": "כותרת אנגלית",
    "UNITITLE_AR": "כותרת ערבית",
    "UNITITLE_AR": "כותרת ערבית",
    "WORKS": "מילות מפתח_יצירות",
    "NUMBER_OF_FILES": "מספר קבצים לאחר דיגיטציה",
    "SECONDARY_UNITITLE": "כותרת משנה",
    "TRANSLATED_UNITITLE": "כותרת מתורגמת",
    "תאריך פקיעת החיסיון": "תאריך פקיעת החיסיון",
}

final_column_order = [
    "סימול אב",
    "ברקוד",
    "סימול מקורי",
    "רמת תיאור",  # 'סימול',
    "מספר מיכל",
    "קוד תיק ארכיון",
    "כותרת",
    "כותרת אנגלית",
    "כותרת ערבית",
    "כותרת מתורגמת",
    "כותרת משנה",
    "תיאור",
    "תאריך חופשי",
    "תאריך מנורמל מוקדם",
    "תאריך מנורמל מאוחר",
    "תאריך יצירת החפץ / הטקסט המקורי מוקדם",
    "תאריך יצירת החפץ / הטקסט המקורי מאוחר",
    "יוצרים",
    "יוצרים אישים",
    "יוצרים מוסדות",
    "מילות מפתח - אישים",
    "מילות מפתח - מוסדות",
    "מילות מפתח_יצירות",
    "מילות מפתח_נושאים",
    "סוג חומר",
    "מדיה + פורמט",
    "קנה מידה",
    "טכניקה",
    "מדינת הפרסום/הצילום",
    "מילות מפתח_מקומות",
    "מגבלות פרטיות",
    "מסלול דיגיטציה",
    "סריקה דו-צדדית",
    "מספר קבצים מוערך",
    "מספר קבצים לאחר דיגיטציה",
    "שפה",
    "היקף החומר",
    "משך",
    "הערות גלוי למשתמש קצה",
    "הערות לא גלוי למשתמש",
    "שם הרושם",
    "תאריך הרישום",
    "היסטוריה ארכיונית",
    "בעלים נוכחי",
    "תיאור הטיפול באוסף בפרויקט",
    "סוג אוסף",
    "אוסף פתוח",
    "ביבליוגרפיה ומקורות מידע",
    "מיקום פיזי",
    "חומרים קשורים",
    "תאריך פקיעת החיסיון",
]

field_types_dict = {
    "date": [
        "CATALOGUING_DATE",
        "DATE_END",
        "DATE_NORMAL",
        "DATE_START",
        "EARLY_NORMAL_DATE",
        "LATE_NORMAL_DATE",
        "DATE_CATALOGING",
    ],
    "number": ["BOX", "CONTAINER"],
    "text": [
        "BIBLIOGRAPHY",
        "BARCODE",
        "BIOGHIST",
        "EXTENT",
        "NOTES",
        "NOTES_HIDDEN",
        "RELATED_MATERIALS",
        "COMBINED_CREATORS",
        "FIRST_CREATOR_PERS",
        "FIRST_CREATOR_CORP",
        "UNITITLE",
        "UNITITLE_ENG’",
        "DIMENSIONS",
        "ARRANGEMENT",
        "EST_FILES_NUM",
        "ORIGINAL_ID",
        "ARCHIV_ID",
        "Parent",
        "PHYSLOC",
        "PROJECT_ID",
        "ROOTID",
        "SCOPECONTENT",
        "STAGE",
        "",
        "UNITID",
        "UNITITLE",
        "TO_DELETE",
        "DATE",
        "APPRAISAL",
        "ARCHIV_ID",
    ],
    "value_list": [
        "CATALOGUER",
        "ADD_CREATORS",
        "ADD_CREATORS_CORPS",
        "ARCHIVAL_MATERIAL",
        "COMBINED_CREATORS",
        "COMBINED_CREATORS_CORPS",
        "COMBINED_CREATORS_PERS",
        "CORPNAME",
        "CREATOR_CORPS",
        "CREATOR_PERS",
        "DIMENSIONS",
        "GEOGNAME",
        "PUBLICATION_COUNTRY",
        "MEDIUM_FORMAT",
        "PERSNAME",
        "SCALE",
        "SUBJECT",
        "WORKS",
        "SIZE",
        "TYPE_FIRST_CREATOR_PERS",
        "TYPE_FIRST_CREATOR_CORP",
        "COLLECTION_TYPE",
    ],
}

copyright_profiles = {"Library premises only": "000000008", "Staff only": "000000018"}


class FieldMapper:
    def __init__(self):
        self.field_mapper = catalog_field_mapper
        self.field_mapper_back = field_mapper_back
        self.level_mapper = level_mapper
        self.collection_field_mapper = collection_field_mapper
