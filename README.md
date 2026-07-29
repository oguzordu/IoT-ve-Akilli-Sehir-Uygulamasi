# Smart City IoT Pipeline

## Türkçe

Akıllı şehir sensör ağını modelleyen simüle bir IoT veri hattı: bir cihaz
ölçüm verisini buluta gönderiyor, bulut da bu veriyi işleyip saklıyor.

### Mimari

```
simulated_device.py → AWS IoT Core → lambda_function.py → DynamoDB
```

- **`simulated_device.py`** — sensör ölçümleri üretip bunları bir cihaz
  sertifikasıyla kimlik doğrulayarak MQTT üzerinden AWS IoT Core'a gönderiyor.
- **AWS IoT Core** — ölçümleri alıp bir IoT kuralı üzerinden bir Lambda
  fonksiyonuna yönlendiriyor.
- **`lambda_function.py`** — gelen her ölçümü DynamoDB'ye yazan bir AWS
  Lambda fonksiyonu.
- **`SmartCityData`** — ölçümlerin saklandığı DynamoDB tablosu.

### Kullanılan Teknolojiler

Python, AWS IoT Core, AWS Lambda, DynamoDB, MQTT, X.509 cihaz sertifikaları.

### Not

Bu proje, artık kapatılmış bir AWS hesabı üzerinde ders projesi olarak
geliştirildi; pipeline şu an deploy edilmiş durumda değil. Repodaki cihaz
sertifikaları etkisiz durumda (hesap artık mevcut değil) ve sadece referans
amaçlı tutuluyor — `certs/*-private.pem.key` yine de git-ignore edilmiş
durumda.

---

## English

A simulated IoT data pipeline modeled on a smart-city sensor network: a
device publishes readings to the cloud, and the cloud processes and stores
them.

### Architecture

```
simulated_device.py → AWS IoT Core → lambda_function.py → DynamoDB
```

- **`simulated_device.py`** — generates sensor readings and publishes them to
  AWS IoT Core over MQTT, authenticated with a device certificate.
- **AWS IoT Core** — receives the readings and routes them to a Lambda
  function via an IoT rule.
- **`lambda_function.py`** — an AWS Lambda function that writes each incoming
  reading to DynamoDB.
- **`SmartCityData`** — the DynamoDB table where readings are stored.

### Tech Stack

Python, AWS IoT Core, AWS Lambda, DynamoDB, MQTT, X.509 device certificates.

### Note

This was built as a coursework project on an AWS account that has since been
closed; the pipeline is not currently deployed. Device certificates in this
repo are inert (the account no longer exists) and are kept for reference
only — `certs/*-private.pem.key` is git-ignored going forward regardless.
