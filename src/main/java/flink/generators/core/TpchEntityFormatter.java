/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package flink.generators.core;

import io.airlift.tpch.LineItem;
import io.airlift.tpch.TpchEntity;
import org.apache.flink.api.java.io.TextOutputFormat;

import static io.airlift.tpch.GenerateUtils.formatDate;
import static io.airlift.tpch.GenerateUtils.formatMoney;

public class TpchEntityFormatter<T extends TpchEntity> implements TextOutputFormat.TextFormatter<T> {

	@Override
	public String format(T value) {
		if(value instanceof LineItem) {
			final StringBuilder lisb = new StringBuilder();
			final LineItem li = (LineItem) value;
			lisb.append(li.getOrderKey());
			lisb.append('|');
			lisb.append(li.getPartKey());
			lisb.append('|');
			lisb.append(li.getSupplierKey());
			lisb.append('|');
			lisb.append(li.getLineNumber());
			lisb.append('|');
			lisb.append(formatMoney((long) (li.getExtendedPrice()*100)));
			lisb.append('|');
			lisb.append(formatMoney((long) (li.getDiscount() * 100)));
			lisb.append('|');
			lisb.append(formatMoney((long) (li.getTax() * 100)));
			lisb.append('|');
			lisb.append(li.getReturnFlag());
			lisb.append('|');
			lisb.append(li.getStatus());
			lisb.append('|');
			lisb.append(formatDate(li.getShipDate()));
			lisb.append('|');
			lisb.append(formatDate(li.getCommitDate()));
			lisb.append('|');
			lisb.append(formatDate(li.getReceiptDate()));
			lisb.append('|');
			lisb.append(li.getShipInstructions());
			lisb.append('|');
			lisb.append(li.getShipMode());
			lisb.append('|');
			lisb.append(li.getComment());
			lisb.append('|');
			return lisb.toString();
		} else {
			return value.toLine();
		}
	}
}
