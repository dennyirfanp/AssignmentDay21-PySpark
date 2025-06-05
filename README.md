# Isi konten markdown-nya
content = """
# Assigment - Day 21 - PySpark.

Phase 1 : Ekstrak Data ke dalam DataFrame
- Muat dataset ke dalam PySpark DataFrame menggunakan "spark.read.csv"
- Periksa tipe data setiap kolom dan sesuaikan jika diperlukan menggunakan "printSchema()"

Phase 2 : Cleaning Data
- Tangani missing values dengan metode yang sesuai menggunakan ".na.drop"
- Hapus data duplikat jika ditemukan menggunakan ".dropDuplicates"
- Perbaiki format data yang tidak sesuai menggunakan "trim" dan "regexp_replace"

Phase 3 : Transformasi Data
- Menggunakan JOIN antar tabel df2 dan df3 untuk mendapatkan informasi yang lebih detail.
- Buat kolom baru yaitu customer_yearly_points

Phase 4: Analisis Data Menggunakan PySpark SQL
- Membuat temporary view untuk setiap DataFrame
- Menganalisa rata-rata jumlah penerbangan per pelanggan dalam setahun 
    dengan Query SQL:
  
     SELECT 
        AVG(total_flights_per_year) AS avg_flights_per_customer_per_year
    FROM (
        SELECT 
            loyalty_number, 
            year, 
            SUM(total_flights) AS total_flights_per_year
        FROM flight_activity
        GROUP BY loyalty_number, year

- Menganalisa distribusi loyalty points berdasarkan status kartu loyalitas 
    dengan Query SQL:

     SELECT 
        loyalty_card,
        AVG(points_accumulated) AS avg_points,
        MIN(points_accumulated) AS min_points,
        MAX(points_accumulated) AS max_points
    FROM joined_data
    GROUP BY loyalty_card

- Menganalisa hubungan antara tingkat pendidikan pelanggan dengan jumlah penerbangan yang mereka lakukan
    dengan Query SQL:

    SELECT 
        education,
        AVG(total_flights) AS avg_flights
    FROM joined_data
    GROUP BY education
    ORDER BY avg_flights DESC


- Menganalisa tren jumlah penerbangan dari waktu ke waktu
    dengan Query SQL:
    
     SELECT 
        year,
        month,
        SUM(total_flights) AS monthly_total_flights
    FROM flight_activity
    GROUP BY year, month
    ORDER BY year, month


Phase 5 : Visualisasi Data
- Memvisualisasikan Tren jumlah penerbangan dari waktu ke waktu dengan kesimpulan bahwa penerbangan terbanyak ada di bulan Juli tahun 2018 dan penerbangan terendah ada di bulan Januari tahun 2017
- Memvisualisasikan Distribusi loyalty points berdasarkan loyalty card dengan kesimpulan loyalty card "Aurora" mempunyai rata-rata loyalty points tertinggi dibandingkan dengan loyalty cards lainnya
- Memvisualisasikan Rata-rata penerbangan berdasarkan tingkat pendidikan dengan kesimpulan bahwa Penerbangan terbanyak per tahun ada di tingkat pendidikan "Highschool or Below"
"""

