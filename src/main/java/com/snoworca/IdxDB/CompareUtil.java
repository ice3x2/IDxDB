package com.snoworca.IdxDB;

public class CompareUtil {

    public static boolean compare(Object origin,Object value, OP op) {
        if(origin instanceof CharSequence) {
            return compareCharSequence((CharSequence) origin,value, op);
        } else if(origin instanceof Long) {
            return compareLong((Long)origin,value,op);
        }
        else if(origin instanceof Integer) {
            return compareInteger((Integer)origin,value,op);
        }
        else if(origin instanceof Short) {
            return compareShort((Short)origin,value,op);
        }
        else if(origin instanceof Byte) {
            return compareByte((Byte)origin,value,op);
        }
        else if(origin instanceof Character) {
            return compareShort((short)((Character)origin).charValue(),value,op);
        }
        else if(origin instanceof Boolean) {
            return compareCharSequence(Boolean.toString((Boolean)origin).toLowerCase(),(value + "").toLowerCase(), op);
        }
        else if(origin instanceof Double) {
            return compareDouble((Double) origin,value, op);
        }
        else if(origin instanceof Float) {
            return compareFloat((Float) origin,value, op);
        }
        return false;

    }

    public static boolean compareCharSequence(CharSequence originValue, Object obj, OP op) {
        String value = obj +"";
        switch (op) {
            case eq:
                return value.equals(originValue);
            case ne:
                return !value.equals(originValue);
            //case in:
            //    return originValue.toString().contains(value);
            //case nin:
             //   return !originValue.toString().contains(value);
            case gt:
                return originValue.toString().compareToIgnoreCase(value) > 0;
            case gte:
                return originValue.toString().compareToIgnoreCase(value) >= 0;
            case lt:
                return originValue.toString().compareToIgnoreCase(value) < 0;
            case lte:
                return originValue.toString().compareToIgnoreCase(value) <=0;
        }
        return false;
    }

    public static boolean compareInteger(Integer originValue, Object obj, OP op) {

        int value = 0;
        if(obj instanceof Number) {
            value =((Number)obj).intValue();
        } else {
            try {
                value = Integer.parseInt(obj + "");
            } catch (NumberFormatException e) {
                return false;
            }
        }
        switch (op) {
            case eq:
                return value == originValue;
            case ne:
                return value != originValue;
            //case nin:
            case gt:
                return originValue > value;
            case gte:
                return originValue >= value;
            case lt:
                return originValue < value;
            //case in:
            case lte:
                return originValue <= value;
        }
        return false;
    }

    public static boolean compareLong(Long originValue, Object obj, OP op) {
        long value = 0;
        if(obj instanceof Number) {
            value =((Number)obj).longValue();
        } else {
            try {
                value = Long.parseLong(obj + "");
            } catch (NumberFormatException e) {
                return false;
            }
        }
        switch (op) {
            case eq:
                return value == originValue;
            case ne:
                return value != originValue;
            //case nin:
            case gt:
                return originValue > value;
            case gte:
                return originValue >= value;
            case lt:
                return originValue < value;
            //case in:
            case lte:
                return originValue <= value;
        }
        return false;
    }


    public static boolean compareShort(Short originValue, Object obj, OP op) {
        short value = 0;
        if(obj instanceof Number) {
            value =((Number)obj).shortValue();
        } else {
            try {
                value = Short.parseShort(obj + "");
            } catch (NumberFormatException e) {
                return false;
            }
        }
        switch (op) {
            case eq:
                return value == originValue;
            case ne:
                return value != originValue;
            //case nin:
            case gt:
                return originValue > value;
            case gte:
                return originValue >= value;
            case lt:
                return originValue < value;
            //case in:
            case lte:
                return originValue <= value;
        }
        return false;
    }

    public static boolean compareByte(Byte originValue, Object obj, OP op) {
        byte value = 0;
        if(obj instanceof Number) {
            value =((Number)obj).byteValue();
        } else {
            try {
                value = Byte.parseByte(obj + "");
            } catch (NumberFormatException e) {
                return false;
            }
        }
        switch (op) {
            case eq:
                return value == originValue;
            case ne:
                return value != originValue;
            //case nin:
            case gt:
                return originValue > value;
            case gte:
                return originValue >= value;
            case lt:
                return originValue < value;
            //case in:
            case lte:
                return originValue <= value;
        }
        return false;
    }


    public static boolean compareFloat(Float originValue, Object obj, OP op) {
        float value = 0;
        if(obj instanceof Number) {
            value =((Number)obj).floatValue();
        } else {
            try {
                value = Float.parseFloat(obj + "");
            } catch (NumberFormatException e) {
                return false;
            }
        }
        switch (op) {
            case eq:
                return originValue.equals(value);
            case ne:
                return !originValue.equals(originValue);
            //case nin:
            case gt:
                return originValue > value;
            case gte:
                return originValue >= value;
            case lt:
                return originValue < value;
            //case in:
            case lte:
                return originValue <= value;
        }
        return false;
    }

    public static boolean compareDouble(Double originValue, Object obj, OP op) {
        double value = 0;
        if(obj instanceof Number) {
            value =((Number)obj).doubleValue();
        } else {
            try {
                value = Double.parseDouble(obj + "");
            } catch (NumberFormatException e) {
                return false;
            }
        }
        switch (op) {
            case eq:
                return originValue.equals(value);
            case ne:
                return !originValue.equals(value);
            //case nin:
            case gt:
                return originValue > value;
            case gte:
                return originValue >= value;
            case lt:
                return originValue < value;
            //case in:
            case lte:
                return originValue <= value;
        }
        return false;
    }


}
