# IDxDB
## 구현 목적
 * **기존 구성의 한계**
   * 개인 프로젝트에 사용하는 MongoDB에 3000여개의 컬렉션을 사용. 가장 큰 컬렉션 하나는 1800만개의 로우를 갖고 있으며 가장 작은 컬렉션도 60만개를 갖고있다. 모든 도큐먼트는 하나의 컬럼에만 인덱스가 걸려있으며, 상위 250만개의 로우는 모두 redis에 캐쉬되어 빠르게 사용할 수 있다. 하지만 리스트를 가져오는 속도가 만족스럽지 못하고, 시스템 자원 사용 빈도도 원하는 것보다 높다.
   * 인덱스를 통한 검색과 업데이트는 매우 빈번하게 일어나지만, 삭제는 거의 발생하지 않으며 삽입또한 많이 발생하지 않는다.(일 20000건 이하)
 * 따라서 프로젝트에 최적화된 아주 간단한 DB를 구현하게 되었다.
## 기능
  * 별 기능 없는 초 간단 DB
  * 데이터는 json형식을 사용하며 저장할 때는 바이너리 형태로 기록됨.
  * 지정된 개수의 상위 데이터를 메모리에 캐쉬한다.
  * 단 하나의 인덱스. 중복을 허용하지 않음.
  * 자바의 TreeSet과 LinkedHashMap을 사용하여 인덱싱.
  * 아직 인덱스외의 값을 이용한 검색은 지원되지않음.
  * json형식의 간단한 쿼리를 지원. 하지만 권한 및 인증과 원격 기능은 구현되지 않음.
## 사용법
  * DB생성

    ```java
    File file = new File("yes.db");
    IdxDB idxDB = IdxDB.newMaker(file).make();
    ```
    
   * 쿼리 사용
     ```java
     String result = idxDB.executeQuery("{json 쿼리 문자열}");
      ```

   * 인덱스셋 생성
     * 인덱스셋은 자바의 TreeSet을 이용함. 중복되는 인덱스를 가질수 없다. 
     ```java
     IndexCollection collection = 
     // 이름을 D005930으로 설정한다.
     idxDB.newIndexSetBuilder("D005930")
     // 인덱스 키를 설정하고 오름차순 정렬
     .index("date", 1)
     // 상위 1000개의 로우를 메모리에 캐시한다
     .memCacheSize(1000)
     // 생성
     .create();
      ```
      
      ```json
     { "method": "newIndexSet", 
       "argument" : {
                      "name": ""D005930",
                      "index": {"date" : 1 },
                      "memCacheSize": 1000
                     }
     }
      ```
      

  * 인덱스맵 생성
     * 인덱스맵은 자바의 LinkedHashmap을 이용하며 실제로 동일하게 동작한다.
     ```java
     IndexCollection collection = 
     // 이름을 FIN_STATE로 설정한다.
     idxDB.newIndexMapBuilder("FIN_STATE")
     // 인덱스 키를 설정.
     // 정렬은 되지 않지만, 리스트를 가져올 때 입력한 순서의 역순으로 가져온다.
     .index("code", -1)
     // 특정 로우에 접근할 경우 해당 값의 순서는 가장 뒤가(혹은 정렬 순서에 따라서 가장 앞)된다.
     .setAccessOrder(true)
     // 상위 1000개의 로우를 메모리에 캐시한다. 
     .memCacheSize(1000)
     // 생성
     .create();
      ```
      ```json
     { "method": "newIndexMap", 
       "argument" : {
                      "name": "FIN_STATE",
                      "index": {"code" : -1 },
                      "accessOrder": true,
                      "memCacheSize": 1000
                     }
     }
      ```
      
  * 데이터 삽입
    * 만약 동일한 index를 갖는 데이터가 있다면, 아무 동작도 발생하지 않는다.
     ```java
      collection.add(new CSONObject().put("date", 20210830).put("start",54300").put("end",57400).put("volume",1013930);
      collection.commit();
     ```
     
