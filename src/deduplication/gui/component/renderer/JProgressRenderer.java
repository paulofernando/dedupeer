package deduplication.gui.component.renderer;

import java.awt.Color;
import java.awt.Component;
import java.awt.Graphics;

import javax.swing.JProgressBar;
import javax.swing.JTable;
import javax.swing.table.TableCellRenderer;

public class JProgressRenderer extends JProgressBar implements TableCellRenderer {

	private static final long serialVersionUID = -4116117091520144073L;
	private Color background, foreground;
	
	@Override
	public Component getTableCellRendererComponent(JTable table, Object value,
			boolean selected, boolean hasFocus, int row, int column) {
		
		if(value != null) 
			this.setValue(((Float) value).intValue());
		
		if(selected) {
			background = table.getSelectionBackground();
		} else {
			background = table.getBackground();
		}
		invalidate();
		return this;
	}
	
	public void paint(Graphics graphics) {
		super.paint(graphics);
		if(this.getValue() == 0) {
			graphics.setColor(background);
			graphics.fillRect(0, 0, this.getWidth(), this.getHeight());
		}
	}
}
