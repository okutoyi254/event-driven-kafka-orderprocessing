package com.example.DistributedKafkaOrderProcessing.inventory;

import com.example.DistributedKafkaOrderProcessing.domain.Entities;
import com.example.DistributedKafkaOrderProcessing.inventory.InventoryItemRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class InventoryDataInitializer implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(InventoryDataInitializer.class);

    private final InventoryItemRepository repository;

    public InventoryDataInitializer(InventoryItemRepository repository) {
        this.repository = repository;
    }

    @Override
    public void run(String... args) {
        if (repository.count() > 0) return; // already seeded

        List<Entities.InventoryItem> items = List.of(
                // Power Tools — good stock
                new Entities.InventoryItem("PROD-DRILL-001", "DeWalt 20V Cordless Drill",
                        "POWER_TOOLS", 50, "AISLE-A1"),
                new Entities.InventoryItem("PROD-SAW-002", "Circular Saw 7-1/4 inch",
                        "POWER_TOOLS", 30, "AISLE-A2"),

                // Plumbing — mixed stock
                new Entities.InventoryItem("PROD-PIPE-010", "3/4 inch Copper Pipe (10ft)",
                        "PLUMBING", 200, "AISLE-B1"),
                new Entities.InventoryItem("PROD-VALVE-011", "Ball Valve 1/2 inch",
                        "PLUMBING", 3, "AISLE-B2"),  // ← low stock — triggers INSUFFICIENT

                // Electrical
                new Entities.InventoryItem("PROD-WIRE-020", "12 AWG Electrical Wire (100ft)",
                        "ELECTRICAL", 75, "AISLE-C1"),
                new Entities.InventoryItem("PROD-PANEL-021", "20A Circuit Breaker",
                        "ELECTRICAL", 40, "AISLE-C2"),

                // Fasteners — high volume
                new Entities.InventoryItem("PROD-NAIL-030", "Framing Nails 3.5 inch (5lb)",
                        "FASTENERS", 500, "AISLE-D1"),
                new Entities.InventoryItem("PROD-SCREW-031", "Deck Screws #10 x 3in (100pk)",
                        "FASTENERS", 300, "AISLE-D2"),

                // Lumber — low stock to show failures
                new Entities.InventoryItem("PROD-LUMB-040", "2x4x8 Pressure Treated Lumber",
                        "LUMBER", 2, "YARD-E1"),     // ← low stock — triggers INSUFFICIENT
                new Entities.InventoryItem("PROD-PLY-041", "3/4 inch Plywood Sheet",
                        "LUMBER", 20, "YARD-E2")
        );

        repository.saveAll(items);
        log.info("✅ Inventory seeded: {} products loaded", items.size());
    }
}