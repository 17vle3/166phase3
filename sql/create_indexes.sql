/*Customer indexes*/
CREATE INDEX index_cutomer_id ON Customer USING btree(id);
CREATE INDEX index_customer_fname ON Customer USING btree(fname);
CREATE INDEX index_customer_lname ON Customer USING btree(lname);
CREATE INDEX index_customer_gtype ON Customer USING btree(gtype);
CREATE INDEX index_customer_dob ON Customer USING btree(dob);
CREATE INDEX index_customer_address ON Customer USING btree(address);
CREATE INDEX index_customer_phone ON Customer USING btree(phone);
CREATE INDEX index_customer_zipcode ON Customer USING btree(zipcode);
/*Captain indexes*/
CREATE INDEX index_captain_id ON Captain USING btree(id);
CREATE INDEX index_captain_fullname ON Captain USING btree(fullname);
CREATE INDEX index_captain_nationality ON Captain USING btree(nationality);
/*Cruise indexes*/
CREATE INDEX index_cruise_cnum ON Cruise USING btree(cnum);
CREATE INDEX index_cruise_cost ON Cruise USING btree(cost);
CREATE INDEX index_cruise_num_sold ON Cruise USING btree(num_sold);
CREATE INDEX index_cruise_num_stops ON Cruise USING btree(num_stops);
CREATE INDEX index_cruise_actual_departure_date ON Cruise USING btree(actual_departure);
CREATE INDEX index_cruise_actual_arrival_time ON Cruise USING btree(actual_arrival_time);
CREATE INDEX index_cruise_arrival_port ON Cruise USING btree(arrival_port);
CREATE INDEX index_cruise_departure_port ON Cruise USING btree(departure_port);
/*Ship indexes*/
CREATE INDEX index_ship_id ON Ship USING btree(id);
CREATE INDEX index_ship_make ON Ship USING btree(make);
CREATE INDEX index_ship_model ON Ship USING btree(model);
CREATE INDEX index_ship_age ON Ship USING btree(age);
CREATE INDEX index_ship_seats ON Ship USING btree(seats);
/*Technician indexes*/
CREATE INDEX index_technician_id ON Technician USING btree(id);
CREATE INDEX index_technician_full_name ON Technician USING btree(full_name);
/*Reservation indexes*/
CREATE INDEX index_reservation_rnum ON Reservation USING btree(rnum);
CREATE INDEX index_reservation_ccid ON Reservation USING btree(ccid);
CREATE INDEX index_reservation_cid ON Reservation USING btree(cid);
CREATE INDEX index_reservation_status ON Reservation USING btree(status);
/*CruiseInfo indexes*/
CREATE INDEX index_cruiseinfo_ciid ON CruiseInfo USING btree(ciid);
CREATE INDEX index_cruiseinfor_cruise_id ON CruiseInfo USING btree(cruise_id);
CREATE INDEX index_cruiseinfo_captain_id ON CruiseInfo USING btree(captain_id);
CREATE INDEX index_cruiseinfo_ship_id ON CruiseInfo USING btree(ship_id);
/*Repairs indexes*/
CREATE INDEX index_repairs_rid ON USING Repairs btree(rid);
CREATE INDEX index_repairs_rapair_date ON USING Repairs btree(repair_date);
CREATE INDEX index_repairs_repair_code ON USING Repairs btree(repair_code);
CREATE INDEX index_repairs_captain_id ON USING Repairs btree(captain_id);
CREATE INDEX index_repairs_ship_id ON USING Repairs btree(ship_id);
CREATE INDEX index_repairs_technician_id ON USING Repairs btree(technician_id);
/*Schedule indexes*/
CREATE INDEX index_schedule_id ON USING Schedule btree(id);
CREATE INDEX index_schedule_cruisenum ON USING Schedule btree(cruisenum);
CREATE INDEX index_schedule_departure_time ON USING Schedule btree(departure_time);
CREATE INDEX index_schedule_arrival_time ON USING Schedule btree(arrival_time);
